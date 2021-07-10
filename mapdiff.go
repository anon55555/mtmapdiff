package main

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

var oldMap, newMap *sql.DB

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage:", os.Args[0], "old new")
		os.Exit(2)
	}

	oldMap = openDB(os.Args[1])
	newMap = openDB(os.Args[2])

	cmp([3]int16{0, 0, 0})
}

func openDB(path string) *sql.DB {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

var done = make(map[[3]int16]bool)

func cmp(blkpos [3]int16) {
	if done[blkpos] {
		return
	}
	done[blkpos] = true

	old := readBlk(oldMap, blkpos)
	if old == nil {
		return
	}

	new := readBlk(newMap, blkpos)
	if new == nil {
		log.Fatal("block disappeared: ", blkpos)
	}

	for i := uint16(0); i < 4096; i++ {
		if new[i] != old[i] {
			p := blkpos2pos(blkpos, i)
			fmt.Println(p[0], p[1], p[2], nodes[old[i]], nodes[new[i]])
		}
	}

	for i := range blkpos {
		a := blkpos
		a[i]++
		cmp(a)

		b := blkpos
		b[i]--
		cmp(b)
	}
}

func blkpos2pos(blkpos [3]int16, i uint16) (pos [3]int16) {
	for j := range pos {
		pos[j] = blkpos[j]<<4 | int16(i>>(4*j)&0xf)
	}

	return
}

func readBlk(db *sql.DB, blkpos [3]int16) *[4096]uint16 {
	be := binary.BigEndian

	var data []byte
	if err := db.QueryRow(`SELECT data FROM blocks WHERE pos = ?;`,
		blkpos2key(blkpos)).Scan(&data); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		log.Fatal(err)
	}

	if len(data) == 0 {
		log.Fatal(io.EOF)
	}
	if v := data[0]; v != 28 {
		log.Fatal("unsupported version: ", v)
	}

	if len(data) < 6 {
		log.Fatal(io.ErrUnexpectedEOF)
	}
	r := bytes.NewReader(data[6:])

	var param0 [4096]uint16
	{
		r, err := zlib.NewReader(r)
		if err != nil {
			log.Fatal("bulk node data: ", err)
		}

		if err := binary.Read(r, be, &param0); err != nil {
			log.Fatal("can't read nodes: ", err)
		}

		if _, err := io.Copy(io.Discard, r); err != nil {
			log.Fatal(err)
		}

		if err := r.Close(); err != nil {
			log.Fatal(err)
		}
	}

	// Node meta.
	{
		r, err := zlib.NewReader(r)
		if err != nil {
			log.Fatal("nodemeta: ", err)
		}

		if _, err := io.Copy(io.Discard, r); err != nil {
			log.Fatal(err)
		}

		if err := r.Close(); err != nil {
			log.Fatal(err)
		}
	}

	buf := make([]byte, 10)
	if _, err := io.ReadFull(r, buf); err != nil {
		log.Fatal(io.ErrUnexpectedEOF)
	}

	// Static objects.
	if v := buf[0]; v != 0 {
		log.Fatal("unsupported static objs version: ", v)
	}
	if be.Uint16(buf[1:3]) != 0 {
		log.Fatal("non-zero static obj count")
	}

	// Name-id mapping.
	if v := buf[7]; v != 0 {
		log.Fatal("unsupported name-id mapping version: ", v)
	}
	ids := make([]uint16, be.Uint16(buf[8:10]))
	for range ids {
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			log.Fatal(io.ErrUnexpectedEOF)
		}
		name := make([]byte, be.Uint16(buf[2:4]))
		if _, err := io.ReadFull(r, name); err != nil {
			log.Fatal(io.ErrUnexpectedEOF)
		}
		ids[be.Uint16(buf[0:2])] = nodeID(string(name)) // Can panic.
	}

	for i, p0 := range param0 {
		param0[i] = ids[p0]
	}

	return &param0
}

func blkpos2key(bp [3]int16) int64 {
	return int64(bp[0]) + int64(bp[1])*4096 + int64(bp[2])*4096*4096
}

var (
	nodeIDs = make(map[string]uint16)
	nodes   []string
)

func nodeID(name string) uint16 {
	id, ok := nodeIDs[name]
	if !ok {
		if len(nodes) > math.MaxUint16 {
			log.Fatal("too many node types")
		}
		id = uint16(len(nodes))
		nodeIDs[name] = id
		nodes = append(nodes, name)
	}
	return id
}
