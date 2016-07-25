package utils

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"os/user"
	"path"
	"strconv"
	"sync"
)

func RotateLog(filename string, pre *os.File) *os.File {
	if filename == "" {
		return pre
	}

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Printf("error opening log file: %s", err)
		return pre
	}
	// log has an internal mutex to guarantee mutual exclusion with log.Write
	log.SetOutput(f)
	if pre != nil {
		pre.Close()
	}
	return f
}

func SavePid(filename string) {
	if filename == "" {
		return
	}

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	defer f.Close()
	if err != nil {
		log.Printf("error opening pid file: %s", err)
		return
	}
	f.Write([]byte(strconv.Itoa(os.Getpid())))
}

type AppData struct {
	FileName string

	data map[string]string
	lock sync.RWMutex
}

func OpenAppDataByFile(f *os.File) (*AppData, error) {
	name := f.Name()
	log.Printf("app data file: %s", name)
	decoder := json.NewDecoder(f)
	v := make(map[string]string)
	e := decoder.Decode(&v)
	if e != nil {
		if e != io.EOF {
			return nil, e
		}
	}
	return &AppData{
		FileName: name,
		data:     v,
	}, nil
}

func OpenAppData(appName string) (*AppData, error) {
	u, e := user.Current()
	if e != nil {
		return nil, e
	}
	name := path.Join(u.HomeDir, "."+appName+".conf")
	log.Printf("app data file: %s", name)
	f, e := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	v := make(map[string]string)
	e = decoder.Decode(&v)
	if e != nil {
		if e != io.EOF {
			return nil, e
		}
	}
	return &AppData{
		FileName: name,
		data:     v,
	}, nil
}

func (a *AppData) Put(key, value string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.data[key] = value
	a.save()
}

func (a *AppData) Del(key string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	delete(a.data, key)
	a.save()

}

func (a *AppData) save() {
	f, e := os.OpenFile(a.FileName, os.O_RDWR|os.O_CREATE, 0666)
	if e != nil {
		return
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	encoder.Encode(a.data)
}

func (a *AppData) Get(key string) (string, bool) {
	value, ok := a.data[key]
	return value, ok
}
