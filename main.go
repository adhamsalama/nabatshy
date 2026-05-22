package main

import (
	"embed"

	"nabatshy/api"
	"nabatshy/collector"
	"nabatshy/db"
	"nabatshy/utils"
)

//go:embed ui/dist/*
var content embed.FS

const uiDir = "ui/dist"

func main() {
	utils.LoadEnv(".env")

	sqlDB := db.InitDuckDB()
	go func() { collector.Run(sqlDB) }()
	go utils.ServeUI(content, uiDir)
	api.Run(sqlDB)
}
