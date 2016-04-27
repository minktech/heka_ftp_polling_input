/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Liu Ming (extrafliu@gmail.com)
#
# ***** END LICENSE BLOCK *****/

package heka_ftp_polling_input

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"github.com/jlaffaye/ftp"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FtpPollingInput struct {
	*FtpPollingInputConfig
	stop     chan bool
	runner   pipeline.InputRunner
	hostname string
	client   *ftp.ServerConn
}

type FtpPollingInputConfig struct {
	TickerInterval uint   `toml:"ticker_interval"`
	Repeat         bool   `toml:"repeat"`
	RepeatMarkDir  string `toml:"repeat_mark_dir"`
	Suffix         string `toml:"suffix"`
	MaxPacket      int    `toml:"max_packet"`
	Host           string `toml:"host"`
	Port           int    `toml:"port"`
	User           string `toml:"user"`
	Pass           string `toml:"password"`
	Compress       bool   `toml:"compress"`
	Dir            string `toml:"dir"`
}

func (input *FtpPollingInput) ConfigStruct() interface{} {
	return &FtpPollingInputConfig{
		TickerInterval: uint(5),
		Repeat:         false,
		MaxPacket:      1 << 15,
		Port:           22,
		User:           "anonymous",
		Pass:           "anonymous",
		RepeatMarkDir:  "dir_polling_input_repeat_mark",
	}
}

func (input *FtpPollingInput) Init(config interface{}) (err error) {
	conf := config.(*FtpPollingInputConfig)
	input.FtpPollingInputConfig = conf
	input.stop = make(chan bool)
	addr := fmt.Sprintf("%s:%d", conf.Host, conf.Port)

	if input.client, err = ftp.Dial(addr); err == nil {
		if err = input.client.Login(conf.User, conf.Pass); err != nil {
			log.Fatalf("unable to start ftp subsytem: %v", err)
			if err = input.client.ChangeDir(conf.Dir); err != nil {
				log.Fatalf("unable to change ftp dir: %v", err)
			}
		}
	} else {
		log.Fatalf("unable to connect to [%s]: %v", addr, err)
	}
	return
}

func (input *FtpPollingInput) Stop() {
	input.client.Logout()
	close(input.stop)
}

func (input *FtpPollingInput) packDecorator(pack *pipeline.PipelinePack) {
	pack.Message.SetType("heka.ftp.polling")
}

func (input *FtpPollingInput) Run(runner pipeline.InputRunner,
	helper pipeline.PluginHelper) error {

	input.runner = runner
	input.hostname = helper.PipelineConfig().Hostname()
	tickChan := runner.Ticker()
	sRunner := runner.NewSplitterRunner("")
	if !sRunner.UseMsgBytes() {
		sRunner.SetPackDecorator(input.packDecorator)
	}

	for {
		select {
		case <-input.stop:
			return nil
		case <-tickChan:
		}

		runner.LogMessage("start polling from sftp")
		if err := input.polling(func(f io.ReadCloser, name string) error {
			return input.read(f, name, runner)
		}); err != nil {
			runner.LogError(err)
			return nil
		}
	}
}

func (input *FtpPollingInput) polling(fn func(io.ReadCloser, string) error) error {
	if entries, err := input.client.List("."); err == nil {
		for _, entry := range entries {
			if entry.Type != ftp.EntryTypeFolder && (input.FtpPollingInputConfig.Suffix == "" || strings.HasSuffix(entry.Name, input.FtpPollingInputConfig.Suffix)) {
				mark := filepath.Join(input.FtpPollingInputConfig.RepeatMarkDir, input.FtpPollingInputConfig.Dir, entry.Name+".ok")
				if !input.FtpPollingInputConfig.Repeat {
					if _, err := os.Stat(mark); !os.IsNotExist(err) {
						return fmt.Errorf("repeat file %s", entry.Name)
					}
				}
				dir := filepath.Dir(mark)
				if err = os.MkdirAll(dir, 0774); err != nil {
					return fmt.Errorf("Error opening file: %s", err.Error())
				} else {
					if err := ioutil.WriteFile(mark, []byte(time.Now().String()), 0664); err != nil {
						return err
					}
				}

				f, err := input.client.Retr(entry.Name)

				if err != nil {
					return fmt.Errorf("Error opening remote file: %s", err.Error())
				}

				defer f.Close()
				err = fn(f, entry.Name)
			}
		}
	}

	return nil
}

func (input *FtpPollingInput) read(file io.ReadCloser, name string, runner pipeline.InputRunner) (err error) {
	sRunner := runner.NewSplitterRunner(name)
	if !sRunner.UseMsgBytes() {
		sRunner.SetPackDecorator(func(pack *pipeline.PipelinePack) {
			pack.Message.SetType("heka.sftp.polling")
			pack.Message.SetHostname(input.hostname)
			if field, err := message.NewField("file_name", name, ""); err == nil {
				pack.Message.AddField(field)
			}
			return
		})
	}

	if input.FtpPollingInputConfig.Compress && input.FtpPollingInputConfig.Suffix == ".gz" {

		var fileReader *gzip.Reader
		if fileReader, err = gzip.NewReader(file); err == nil {
			defer fileReader.Close()
			tarBallReader := tar.NewReader(fileReader)
			for {
				if header, err := tarBallReader.Next(); err == nil {
					// get the individual filename and extract to the current directory
					switch header.Typeflag {
					case tar.TypeReg:
						split(runner, sRunner, tarBallReader)
					}
				} else {
					break
				}
			}
		}
	} else {
		split(runner, sRunner, file)
	}
	return
}

func split(runner pipeline.InputRunner, sRunner pipeline.SplitterRunner, reader io.Reader) {
	var err error
	for err == nil {
		err = sRunner.SplitStream(reader, nil)
		if err != io.EOF && err != nil {
			runner.LogError(fmt.Errorf("Error reading file: %s", err.Error()))
		}
	}
}

func init() {
	pipeline.RegisterPlugin("FtpPollingInput", func() interface{} {
		return new(FtpPollingInput)
	})
}
