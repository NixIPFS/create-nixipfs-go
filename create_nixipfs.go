package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"github.com/NixIPFS/go-ipfs-api"
	"encoding/csv"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"
)

var (
	sh            *shell.Shell
	ip            = flag.String("ipfsapi", "127.0.0.1:5001", "ipfs api location")
	localDir      = flag.String("dir", "output", "location of local nixipfs path")
	hashCache     map[string]string
	hashCacheFile string
	nixfsDir      string
	channelsDir   string
	releasesDir   string
	validReleasePaths []string
	hashableReleaseExtensions []string
)

func init() {
	flag.Parse()
	hashCacheFile = filepath.Join(*localDir, "ipfs_hashes")
	channelsDir = filepath.Join(*localDir, "channels")
	releasesDir = filepath.Join(*localDir, "releases")
	nixfsDir = fmt.Sprintf("/nixfs_%d", time.Now().Unix())
	hashCache = make(map[string]string)
	validReleasePaths = []string{ "binary-cache-url", "git-revision", "nixexprs.tar.xz", ".ova", ".iso", "src-url", "store-paths.xz" }
	hashableReleaseExtensions = []string{ ".iso", ".ova" }
}

type Pair struct {
	a, b string
}

func AddToIPFS(c chan Pair, s *shell.Shell, h string, narDir string, file string) {
	fr, err := os.Open(filepath.Join(narDir, file))
	if err != nil {
		log.Fatal(err)
	}
	hash, err := sh.AddWithOpts(fr, false, true)
	if err != nil {
		log.Fatal(err)
	}
	c <- Pair{file, hash}
	return
}

func AddDirToIPFS (dir string, mfsDir string, extensions []string, cached_extensions []string) {
	d0 := make(chan Pair)
	filteredFiles := make([]string, 0)
	localCache := make(map[string]string)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		for _, extension := range extensions {
			if strings.Contains(f.Name(), extension) {
				filteredFiles = append(filteredFiles, f.Name())
			}
		}
	}

	for _, f := range filteredFiles {
		go AddToIPFS(d0, sh, hashCache[f], dir, f)
	}

	for i := 0; i < len(filteredFiles); i++ {
		x := <- d0
		cache := false
		for _, extension := range cached_extensions {
			if strings.Contains(x.a, extension) {
				cache = true
			}
		}
		if (cache) {
			hashCache[x.a] = x.b
		} else {
			localCache[x.a] = x.b
		}
	}

	for _, f := range filteredFiles {
		h := hashCache[f]
		if h == "" {
			h = localCache[f]
		}
		if h == "" {
			log.Fatal("AddToIPFS failed: should not happen")
		}
		err := sh.FilesCp("/ipfs/"+h, filepath.Join(mfsDir, f), false)
		if err != nil {
			log.Fatal(err)
		}
	}
}


func createEmptyDir(sh *shell.Shell) (empty string, err error) {
	empty, err = sh.NewObject("unixfs-dir")
	return
}

func addBinaryCache(sh *shell.Shell, localDir string, mfsDir string, hashCache map[string]string) () {
	binaryCacheDir := filepath.Join(localDir, "binary_cache")
	narDir := filepath.Join(binaryCacheDir, "nar")

	mfsBinaryCacheDir := filepath.Join(mfsDir, "binary_cache")
	mfsNarDir := filepath.Join(mfsBinaryCacheDir, "nar")

	err := sh.FilesMkdir(mfsBinaryCacheDir, false)
	if err != nil {
		log.Fatal(err)
	}
	err = sh.FilesMkdir(mfsNarDir, false)
	if err != nil {
		log.Fatal(err)
	}

	AddDirToIPFS(binaryCacheDir, mfsBinaryCacheDir, []string{".narinfo"}, []string{".narinfo"})
	AddDirToIPFS(narDir, mfsNarDir, []string{".nar","nar-cache-info"}, []string{".nar"})
	sh.FilesFlush(mfsBinaryCacheDir)
}

func addNixosRelease(sh *shell.Shell, localDir string, mfsDir string, hashCache map[string]string) () {
	hashFile := filepath.Join(localDir, "ipfs_hash")

	if _, err := os.Stat(hashFile); !os.IsNotExist(err) {
		hash, err := ioutil.ReadFile(hashFile)
		if err != nil {
			log.Fatal(err)
		}
		err = sh.FilesMkdir(filepath.Dir(mfsDir), true)
		if err != nil {
			log.Fatal(err)
		}

		err = sh.FilesCp("/ipfs/"+string(hash), mfsDir, false)
		if err != nil {
			log.Fatal(err)
		}
	        sh.FilesFlush(mfsDir)
	} else {
		err := sh.FilesMkdir(mfsDir, true)
		if err != nil {
			log.Fatal(err)
		}
		AddDirToIPFS(localDir, mfsDir, validReleasePaths, hashableReleaseExtensions)
		addBinaryCache(sh, localDir, mfsDir, hashCache)
	        sh.FilesFlush(mfsDir)

		res, err := sh.FilesStat(mfsDir)
		if err != nil {
			log.Fatal(err)
		}
		hash := res.Hash

		f, err := os.Create(hashFile)
		if err != nil {
			log.Fatal(err)
		}
		f.WriteString(hash)
		f.Sync()
		f.Close()
	}
}

func listDirs(path string) ([]string, error) {
	result := make([]string, 0)
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	} else {
		for _, entry := range dir {
			if entry.IsDir() || entry.Mode()&os.ModeSymlink != 0 {
				result = append(result, filepath.Join(path, entry.Name()))
			}
		}
	}
	return result, nil
}

func main() {
	sh = shell.NewShell(*ip)

	f, err := ioutil.ReadFile(hashCacheFile)
	if err == nil {
		r := csv.NewReader(strings.NewReader(string(f)))
		r.Comma = ':'
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			hashCache[record[0]] = record[1]
		}
	} else {
		if !os.IsNotExist(err) {
			log.Fatal(err)
		}
	}
	err = sh.FilesMkdir(nixfsDir + "/channels", true)
	if err != nil {
		log.Fatal(err)
	}
	err = sh.FilesMkdir(nixfsDir + "/releases", true)
	if err != nil {
		log.Fatal(err)
	}
	// add global binary cache
	log.Printf("Adding global cache...")
	addBinaryCache(sh, *localDir, nixfsDir, hashCache)
	releaseNames, err := listDirs(releasesDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, releaseName := range releaseNames {
		releaseDirs, err := listDirs(releaseName)
		if err != nil {
			log.Fatal(err)
		}
		for _, releaseDir := range releaseDirs {
			log.Printf("Adding release: " + filepath.Base(releaseDir))
			addNixosRelease(sh, releaseDir, filepath.Join(nixfsDir, "releases", filepath.Base(releaseName), filepath.Base(releaseDir)), hashCache)
		}
	}
	channelNames, err := listDirs(channelsDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, channelName := range channelNames {
		log.Printf("Adding channel: " + filepath.Base(channelName))
		addNixosRelease(sh, channelName, filepath.Join(nixfsDir, "channels", filepath.Base(channelName)), hashCache)
	}

	log.Printf("Flushing")
	sh.FilesFlush(nixfsDir)

	fsStatRes, err := sh.FilesStat(nixfsDir)
	if err != nil {
		log.Fatal(err)
	}
	hash := fsStatRes.Hash

	log.Printf("Pinning")
	sh.Pin(hash)

	nsPublishRes, err := sh.NamePublish("/ipfs/" + hash)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Published %s to /ipns/%s", nsPublishRes.Value, nsPublishRes.Name)

	hcf, err := os.Create(hashCacheFile)
	if err != nil {
		log.Fatal(err)
	}

	hcfw := csv.NewWriter(hcf)
	hcfw.Comma = ':'

	for k, v := range hashCache {
		err := hcfw.Write([]string{k,v})
		if err != nil {
			log.Fatal(err)
		}
	}

	hcfw.Flush()
	hcf.Close()
}
