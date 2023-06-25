module Jedis

export Client, Pipeline, RedisError, get_global_client, set_global_client, get_ssl_config,
    disconnect!, reconnect!, add!, copy, wait_until_subscribed, wait_until_unsubscribed,
    wait_until_channel_unsubscribed, wait_until_pattern_unsubscribed, execute, auth, select,
    ping, flushdb, flushall, quit, set, setnx, get, del, exists, setex, isclosed, incr, incrby, incrbyfloat,
    expire, ttl, multi, exec, multi_exec, pipeline,
    publish, subscribe, unsubscribe, psubscribe, punsubscribe,
    acquire_lock, release_lock, redis_lock, isredislocked,
    hexists, hkeys, hset, hget, hgetall, hmget, hdel, hincrby, hincrbyfloat,
    rpush, lpush, lpos, lrem, lpop, rpop, blpop, brpop, llen, lrange,
    zincrby, zadd, zrange, zrangebyscore, zrem, xrange,
    xack, xadd, xautoclaim, xclaim, xdel, xgroup_create, xgroup_delconsumer, xgroup_destroy,
    xgroup_createconsumer, xgroup_setid, xinfo_consumers, xinfo_groups, xinfo_stream, xlen,
    xpending, xrange, xread, xreadgroup, xrevrange, xtrim


using Sockets
using MbedTLS
using UUIDs: uuid4
import Base: copy, showerror, get, pipeline

include("exceptions.jl")
include("utilities.jl")
include("stream.jl")
include("client.jl")
include("pipeline.jl")
include("protocol.jl")
include("execute.jl")
include("commands.jl")
include("pubsub.jl")
include("lock.jl")

end # module