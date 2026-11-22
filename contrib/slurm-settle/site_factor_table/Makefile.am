# SPDX-FileCopyrightText: 2026 The YAWN authors
# SPDX-License-Identifier: GPL-2.0-or-later
# Makefile for site_factor/table plugin (drop this directory into src/plugins/site_factor/)

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -DSLURM_PLUGIN_DEBUG -I$(top_srcdir)

pkglib_LTLIBRARIES = site_factor_table.la

site_factor_table_la_SOURCES = site_factor_table.c
site_factor_table_la_LDFLAGS = $(PLUGIN_FLAGS)
