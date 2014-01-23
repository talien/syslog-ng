/*
 * Copyright (c) 2002-2014 BalaBit IT Ltd, Budapest, Hungary
 * Copyright (c) 2014 Viktor Juhasz
 * Copyright (c) 2014 Viktor Tusa
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * As an additional exemption you are allowed to compile & link against the
 * OpenSSL libraries as published by the OpenSSL project. See the file
 * COPYING for details.
 *
 */

#ifndef DUMP_H
#define DUMP_H 1

#include "persist-tool.h"
#include "syslog-ng.h"
#include "mainloop.h"
#include "persist-state.h"
#include "plugin.h"
#include "persistable-state-presenter.h"
#include "cfg.h"

static GOptionEntry dump_options[] =
{
    { NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL }
};

gint dump_main(int argc, char *argv[]);

#endif
