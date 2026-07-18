/*
 * ustp - OpenWrt STP/RSTP/MSTP daemon
 * Copyright (C) 2021 Felix Fietkau <nbd@nbd.name>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2
 * as published by the Free Software Foundation
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */
#include <pthread.h>
#include <limits.h>
#include <string.h>

#include <libubox/uloop.h>
#include <libubox/utils.h>

#include "worker.h"
#include "bridge_ctl.h"
#include "bridge_track.h"
#include "packet.h"

static pthread_t w_thread;
static pthread_mutex_t w_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t w_cond = PTHREAD_COND_INITIALIZER;
static struct worker_event w_queue[WORKER_QUEUE_SIZE];
static unsigned int w_queue_head;
static unsigned int w_queue_count;
static bool w_shutdown;
static struct uloop_timeout w_timer;

static bool worker_event_is_background(enum worker_event_type type)
{
	return type == WORKER_EV_RECV_PACKET ||
	       type == WORKER_EV_BRIDGE_EVENT ||
	       type == WORKER_EV_ONE_SECOND;
}

static int worker_find_pending(enum worker_event_type type)
{
	unsigned int i;

	for (i = 0; i < w_queue_count; i++) {
		unsigned int pos = (w_queue_head + i) % WORKER_QUEUE_SIZE;

		if (w_queue[pos].type == type)
			return pos;
	}

	return -1;
}

static struct worker_event worker_next_event(void)
{
	struct worker_event ev;

	pthread_mutex_lock(&w_lock);
	while (!w_queue_count && !w_shutdown)
		pthread_cond_wait(&w_cond, &w_lock);

	if (!w_queue_count) {
		ev = (struct worker_event) { .type = WORKER_EV_SHUTDOWN };
		pthread_mutex_unlock(&w_lock);
		return ev;
	}

	ev = w_queue[w_queue_head];
	w_queue_head = (w_queue_head + 1) % WORKER_QUEUE_SIZE;
	w_queue_count--;
	pthread_mutex_unlock(&w_lock);

	return ev;
}

static void
handle_worker_event(struct worker_event *ev)
{
	switch (ev->type) {
	case WORKER_EV_ONE_SECOND:
		while (ev->count--)
			bridge_one_second();
		break;
	case WORKER_EV_BRIDGE_EVENT:
		bridge_event_handler();
		break;
	case WORKER_EV_RECV_PACKET:
		packet_rcv();
		break;
	case WORKER_EV_BRIDGE_ADD:
		bridge_create(ev->bridge_idx, &ev->bridge_config);
		break;
	case WORKER_EV_BRIDGE_REMOVE:
		bridge_delete(ev->bridge_idx);
		break;
	default:
		return;
	}
}

static void *worker_thread_fn(void *arg)
{
	struct worker_event ev;

	while (1) {
		ev = worker_next_event();
		if (ev.type == WORKER_EV_SHUTDOWN)
			break;

		handle_worker_event(&ev);
	}

	return NULL;
}

static void worker_timer_cb(struct uloop_timeout *t)
{
	struct worker_event ev = {
		.type = WORKER_EV_ONE_SECOND,
	};

	uloop_timeout_set(t, 1000);
	worker_queue_event(&ev);
}

int worker_init(void)
{
	w_queue_head = 0;
	w_queue_count = 0;
	w_shutdown = false;
	w_timer.cb = worker_timer_cb;
	uloop_timeout_set(&w_timer, 1000);

	return pthread_create(&w_thread, NULL, worker_thread_fn, NULL);
}

void worker_cleanup(void)
{
	pthread_mutex_lock(&w_lock);
	w_shutdown = true;
	pthread_cond_signal(&w_cond);
	pthread_mutex_unlock(&w_lock);
	pthread_join(w_thread, NULL);
}

bool worker_queue_event(const struct worker_event *ev)
{
	unsigned int limit = WORKER_QUEUE_SIZE;
	unsigned int pos;
	int pending;
	bool queued = false;

	pthread_mutex_lock(&w_lock);
	if (w_shutdown)
		goto out;

	if (worker_event_is_background(ev->type)) {
		pending = worker_find_pending(ev->type);
		if (pending >= 0) {
			if (ev->type == WORKER_EV_ONE_SECOND &&
			    w_queue[pending].count < UINT_MAX)
				w_queue[pending].count++;
			queued = true;
			goto out;
		}
		limit -= WORKER_CONTROL_RESERVE;
	}

	if (w_queue_count >= limit)
		goto out;

	pos = (w_queue_head + w_queue_count) % WORKER_QUEUE_SIZE;
	w_queue[pos] = *ev;
	w_queue[pos].count = 1;
	w_queue_count++;
	queued = true;
	pthread_cond_signal(&w_cond);

out:
	pthread_mutex_unlock(&w_lock);

	return queued;
}
