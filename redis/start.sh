#!/bin/bash
redis-server `dirname ${0}`/redis_6789.conf $@
