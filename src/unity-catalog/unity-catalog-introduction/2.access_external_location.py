# Databricks notebook source
ext_location_credential = 'abfss://demo@databrickcourseextdl.dfs.core.windows.net/'

display(dbutils.fs.ls(ext_location_credential))