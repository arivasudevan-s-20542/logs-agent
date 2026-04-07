package api

import (
	"embed"
	"io/fs"
)

//go:embed static/*
var embeddedStatic embed.FS

func staticFS() fs.FS {
	sub, _ := fs.Sub(embeddedStatic, "static")
	return sub
}
