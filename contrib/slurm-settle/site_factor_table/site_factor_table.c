/*****************************************************************************\
 *  site_factor_table.c - set each job's SiteFactor from a table kept by the site
 *****************************************************************************
 *  A site_factor plugin that reads per-user adjustments from a file.
 *
 *  The site writes a small text file; this plugin applies it to jobs:
 *      user delta            e.g.  alice -2500
 *      user account delta    e.g.  alice lab_a -2500     (takes precedence)
 *  delta is added to the job's priority as its SiteFactor (see sprio, SITE column).
 *  Jobs of users not in the table get 0. Lines starting with '#' are ignored.
 *
 *  slurm.conf:
 *      PrioritySiteFactorPlugin=site_factor/table
 *      PrioritySiteFactorParameters=file=/etc/slurm/site_factor.txt,limit=10000
 *  limit caps |delta| (default 10000). The file is re-read when it changes; it is
 *  applied when a job is submitted and to every pending job each
 *  PriorityCalcPeriod. A missing or unreadable file means "no adjustment".
 *
 *  Nothing in Slurm's accounting is changed: usage records and sshare stay an
 *  accurate record of what ran. One use: a gateway in front of a shared service
 *  writes, per user, the fair-share difference that the user's share of the
 *  service would have caused had it been ordinary job usage.
 *
 *  SPDX-FileCopyrightText: 2026 The YAWN authors
 *  Follows the layout of Slurm's src/plugins/site_factor/example
 *  (Copyright (C) SchedMD LLC.).
 *  SPDX-License-Identifier: GPL-2.0-or-later
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 *  more details (COPYING in the parent directory).
\*****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "src/common/slurm_xlator.h"

#include "src/common/log.h"
#include "src/common/read_config.h"
#include "src/common/uid.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/slurmctld/slurmctld.h"

const char plugin_name[] = "site_factor from a table";
const char plugin_type[] = "site_factor/table";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;

typedef struct {
	char *user;
	char *account;	/* NULL: any account */
	int32_t delta;
} entry_t;

static char *table_file = NULL;
static int32_t limit = 10000;
static entry_t *table = NULL;
static int table_cnt = 0;
static struct timespec table_mtime = { 0, 0 };
static bool table_loaded = false;

static void _free_table(void)
{
	for (int i = 0; i < table_cnt; i++) {
		xfree(table[i].user);
		xfree(table[i].account);
	}
	xfree(table);
	table_cnt = 0;
}

/* Re-read the file if it changed. Called with the job write lock held, so keep it short. */
static void _load_table(void)
{
	struct stat st;
	FILE *fp;
	char line[512];

	if (!table_file)
		return;
	if (stat(table_file, &st)) {
		if (table_loaded || table_cnt) {
			info("%s: %s is gone: no adjustments", plugin_type, table_file);
			_free_table();
			table_loaded = false;
		}
		return;
	}
	if (table_loaded && (st.st_mtim.tv_sec == table_mtime.tv_sec) &&
	    (st.st_mtim.tv_nsec == table_mtime.tv_nsec))
		return;
	if (!(fp = fopen(table_file, "r"))) {
		error("%s: cannot read %s: %m", plugin_type, table_file);
		return;
	}
	_free_table();
	while (fgets(line, sizeof(line), fp)) {
		char a[128], b[128], c[128];
		int n;
		long d;
		char *end = NULL;

		if (line[0] == '#')
			continue;
		n = sscanf(line, "%127s %127s %127s", a, b, c);
		if (n < 2)
			continue;
		d = strtol((n == 3) ? c : b, &end, 10);
		if (!end || *end)
			continue;	/* not a number: skip the line */
		if (d > limit)
			d = limit;
		if (d < -limit)
			d = -limit;
		xrecalloc(table, table_cnt + 1, sizeof(entry_t));
		table[table_cnt].user = xstrdup(a);
		table[table_cnt].account = (n == 3) ? xstrdup(b) : NULL;
		table[table_cnt].delta = d;
		table_cnt++;
	}
	fclose(fp);
	table_mtime = st.st_mtim;
	table_loaded = true;
	log_flag(PRIO, "%s: loaded %d entries from %s", plugin_type, table_cnt, table_file);
}

static int32_t _delta(job_record_t *job_ptr)
{
	int32_t any = 0;
	char *user = job_ptr->user_name;

	if (!table_cnt)
		return 0;
	if (!user)	/* not filled in yet when the job is being created */
		user = uid_to_string_cached(job_ptr->user_id);
	if (!user)
		return 0;
	for (int i = 0; i < table_cnt; i++) {
		if (xstrcmp(table[i].user, user))
			continue;
		if (!table[i].account)
			any = table[i].delta;
		else if (!xstrcmp(table[i].account, job_ptr->account))
			return table[i].delta;
	}
	return any;
}

static int _update(void *x, void *ignored)
{
	job_record_t *job_ptr = x;

	if (IS_JOB_PENDING(job_ptr))
		job_ptr->site_factor = NICE_OFFSET + _delta(job_ptr);
	return SLURM_SUCCESS;
}

extern int init(void)
{
	char *params = slurm_conf.site_factor_params, *tmp, *tok, *save = NULL;

	tmp = xstrdup(params);
	for (tok = strtok_r(tmp, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
		if (!xstrncasecmp(tok, "file=", 5))
			table_file = xstrdup(tok + 5);
		else if (!xstrncasecmp(tok, "limit=", 6))
			limit = labs(strtol(tok + 6, NULL, 10));
	}
	xfree(tmp);
	if (!table_file)
		error("%s: PrioritySiteFactorParameters needs file=PATH; no adjustments will be made", plugin_type);
	else
		debug("%s: table %s, limit %d", plugin_type, table_file, limit);
	return SLURM_SUCCESS;
}

extern void fini(void)
{
	_free_table();
	xfree(table_file);
}

extern void site_factor_p_set(job_record_t *job_ptr)
{
	_load_table();
	job_ptr->site_factor = NICE_OFFSET + _delta(job_ptr);
}

extern void site_factor_p_update(void)
{
	_load_table();
	list_for_each(job_list, _update, NULL);
}
