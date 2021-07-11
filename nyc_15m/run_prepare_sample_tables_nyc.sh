#!/usr/bin/env bash

psql -d nyc -U postgres -f prepare_sample_tables_nyc.sql