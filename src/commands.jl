using Printf

"""
    auth(password[, username])

Authenticate to the server.
"""
auth(password, username=""; client=get_global_client()) = execute(["AUTH", username, password], client)

"""
    select(database)

Change the selected database for the current connection.
"""
select(database; client=get_global_client()) = execute(["SELECT", database], client)

"""
    ping()

Ping the server.
"""
ping(; client=get_global_client()) = execute(["PING"], client)

"""
    flushdb([; async=false])

Remove all keys from the current database.
"""
flushdb(; async=false, client=get_global_client()) = execute(["FLUSHDB", async ? "ASYNC" : ""], client)

"""
    flushall([; async=false])

Remove all keys from all databases.
"""
flushall(; async=false, client=get_global_client()) = execute(["FLUSHALL", async ? "ASYNC" : ""], client)

"""
    quit()

Close the connection.
"""
quit(; client=get_global_client()) = execute(["QUIT"], client)

"""
    set(key, value)

Set the string value of a key.
"""
set(key, value; client=get_global_client()) = execute(["SET", key, value], client)

"""
    setnx(key, value)

Set the value of a key, only if the key does not exist.
"""
setnx(key, value; client=get_global_client()) = execute(["SETNX", key, value], client)

"""
    get(key)

Get the value of a key.
"""
Base.get(key; client=get_global_client()) = execute(["GET", key], client)

"""
    del(key[, keys...])

Delete a key.
"""
del(key, keys...; client=get_global_client()) = execute(["DEL", key, keys...], client)

"""
    exists(key[, keys...])

Determine if a key exists.
"""
exists(key, keys...; client=get_global_client()) = execute(["EXISTS", key, keys...], client)

"""
    hexists(key, field)

Determine if a hash field exists.
"""
hexists(key, field; client=get_global_client()) = execute(["HEXISTS", key, field], client)

"""
    keys(pattern)

Find all keys matching the pattern.

!!! compat "Julia 1.6"
    `Jedis.keys` is not exported as the interface conflicts with `Base.keys`.
"""
keys(pattern; client=get_global_client()) = execute(["KEYS", pattern], client)

"""
    hkeys(key)

Get all fields in a hash.
"""
hkeys(key; client=get_global_client()) = execute(["HKEYS", key], client)

"""
    setex(key, seconds, value)

Set the value and expiration of a key.
"""
setex(key, seconds, value; client=get_global_client()) = execute(["SETEX", key, seconds, value], client)

"""
    expire(key, seconds)

Set a key's tiem to live in seconds.
"""
expire(key, seconds; client=get_global_client()) = execute(["EXPIRE", key, seconds], client)

"""
    ttl(key)

Get the time to live for a key.
"""
ttl(key; client=get_global_client()) = execute(["TTL", key], client)

"""
    multi()

Mark the start of a transaction block.

# Examples
```julia-repl
julia> multi()
"OK"

julia> set("key", "value")
"QUEUED"

julia> get("key")
"QUEUED"

julia> exec()
2-element Array{String,1}:
 "OK"
 "value"
```
"""
multi(; client=get_global_client()) = execute(["MULTI"], client)

"""
    exec()

Execute all commands issued after MULTI.

# Examples
```julia-repl
julia> multi()
"OK"

julia> set("key", "value")
"QUEUED"

julia> get("key")
"QUEUED"

julia> exec()
2-element Array{String,1}:
 "OK"
 "value"
```
"""
exec(; client=get_global_client()) = execute(["EXEC"], client)

"""
    multi_exec(fn::Function)

Execute a MULTI/EXEC transction in a do block.

# Examples
```julia-repl
julia> multi_exec() do 
           set("key", "value")
           get("key")
           get("key")
       end
3-element Array{String,1}:
 "OK"
 "value"
 "value"
```
"""
multi_exec(fn::Function; client=get_global_client()) = (multi(; client=client); fn(); exec(; client=client))

"""
    hset(key, field, value)

Set the string value of a hash field.
"""
hset(key, field, value, fields_and_values...; client=get_global_client()) = execute(["HSET", key, field, value, fields_and_values...], client)

"""
    hget(key, field)

Get the value of a hash field.
"""
hget(key, field; client=get_global_client()) = execute(["HGET", key, field], client)

"""
    hgetall(key)

Get all the fields and values in a hash.
"""
hgetall(key; client=get_global_client()) = execute(["HGETALL", key], client)

"""
    hmget(key, field[, fields...])

Get the values of all the given hash fields.
"""
hmget(key, field, fields...; client=get_global_client()) = execute(["HMGET", key, field, fields...], client)

"""
    hdel(key, field[, fields...])

Delete one or more hash fields.
"""
hdel(key, field, fields...; client=get_global_client()) = execute(["HDEL", key, field, fields...], client)

"""
    lpush(key, element[, elements...])

Prepend one or multiple elements to a list.
"""
lpush(key, element, elements...; client=get_global_client()) = execute(["LPUSH", key, element, elements...], client)

"""
    rpush(key, element[, elements...])

Append one or multiple elements to a list.
"""
rpush(key, element, elements...; client=get_global_client()) = execute(["RPUSH", key, element, elements...], client)

"""
    lpos(key, element[, rank, num_matches, len])

Return the index of matching element on a list.
"""
lpos(key, element, rank="", num_matches="", len=""; client=get_global_client()) = execute(["LPOS", key, element, [isempty(rank) ? "" : "RANK", rank]..., [isempty(num_matches) ? "" : "COUNT", num_matches]..., [isempty(len) ? "" : "MAXLEN", len]...], client)

"""
    lrem(key, count, element)

Remove elements from a list.
"""
lrem(key, count, element; client=get_global_client()) = execute(["LREM", key, count, element], client)

"""
    lpop(key)

Remove and get the first element in a list.
"""
lpop(key; client=get_global_client()) = execute(["LPOP", key], client)

"""
    rpop(key)

Remove and get the last element in a list.
"""
rpop(key; client=get_global_client()) = execute(["RPOP", key], client)

"""
    blpop(key[, key...; timeout=0])

Remove and get the first element in a list, or block until one is available.
"""
blpop(key, keys...; timeout=0, client=get_global_client()) = execute(["BLPOP", key, keys..., timeout], client)

"""
    brpop(key[, key...; timeout=0])

Remove and get the last element in a list, or block until one is available.
"""
brpop(key, keys...; timeout=0, client=get_global_client()) = execute(["BRPOP", key, keys..., timeout], client)

"""
    llen(key)

Get the length of a list.
"""
llen(key; client=get_global_client()) = execute(["LLEN", key], client)

"""
    lrange(key, start, stop)

Get a range of elements from a list.
"""
lrange(key, start, stop; client=get_global_client()) = execute(["LRANGE", key, start, stop], client)

"""
    incr(key)

Increment the integer value of a key by one.
"""
incr(key; client=get_global_client()) = execute(["INCR", key], client)

"""
    incrby(key, increment)

Increment the integer value of a key by the given amount.
"""
incrby(key, increment; client=get_global_client()) = execute(["INCRBY", key, increment], client)

"""
    incrbyfloat(key, increment)

Increment the float value of a key by the given amount.
"""
incrbyfloat(key, increment; client=get_global_client()) = execute(["INCRBYFLOAT", key, increment], client)

"""
    hincrby(key, field, increment)

Increment the integer value of a hash field by the given number.
"""
hincrby(key, increment; client=get_global_client()) = execute(["HINCRBY", key, field, increment], client)

"""
    hincrbyfloat(key, field, increment)

Increment the float value of a hash field by the given number.
"""
hincrbyfloat(key, field, increment; client=get_global_client()) = execute(["HINCRBYFLOAT", key, field, increment], client)

"""
    zincrby(key, field, member)

Increment the score of a member in a sorted set.
"""
zincrby(key, field, increment; client=get_global_client()) = execute(["ZINCRBY", key, field, increment], client)

"""
    zadd(key, score, member[, score_and_members...])

Add one or more members to a sorted set, or update its score if it 
already exists.
"""
zadd(key, score, member, score_and_members...; client=get_global_client()) = execute(["ZADD", key, score, member, score_and_members...], client)

"""
    zrange(key, min, max)

Store a range of members from sorted set into another key.
"""
zrange(key, min, max; client=get_global_client()) = execute(["ZRANGE", key, min, max], client)

"""
    zrangebyscore(key, min, max)

Return a range of members in a sorted set, by score.
"""
zrangebyscore(key, min, max; client=get_global_client()) = execute(["ZRANGEBYSCORE", key, min, max], client)

"""
    zrem(key, member[, members...])

Remove one or more members from a sorted set.
"""
zrem(key, member, members...; client=get_global_client()) = execute(["ZREM", key, member, members...], client)


function xack(name::AbstractString, groupname::AbstractString, ids::AbstractString...; client=get_global_client())
    execute(["XACK", name, groupname, ids...], client)
end

function xadd(key::AbstractString, members::Union{Nothing,AbstractDict,NamedTuple}=nothing; id::AbstractString="*", maxlen::Union{Integer,Nothing}=nothing, approximate::Bool=false, nomkstream::Bool=false, minid::Union{AbstractString,Nothing}=nothing, limit::Union{Integer,Nothing}=nothing, client=get_global_client(), members_kw...)
    if members === nothing && isempty(members_kw)
        throw(ArgumentError("Either `members` or `members_kw...` must be specified"))
    end
    command::Vector{Any} = ["XADD", key]
    if maxlen !== nothing
        if minid !== nothing
            throw(ArgumentError("Only one of `maxlen` or `minid` may be specified"))
        end
        push!(command, "MAXLEN")
        if approximate
            push!(command, "~")
        end
        push!(command, @sprintf("%i", maxlen))
    elseif minid !== nothing
        push!(command, "MINID")
        if approximate
            push!(command, "~")
        end
        push!(command, minid)
    end
    if limit !== nothing
        push!(command, "LIMIT", @sprintf("%i", limit))
    end
    if nomkstream
        push!(command, "NOMKSTREAM")
    end
    push!(command, id)
    if members !== nothing
        append!(command, pairs(members)...)
    end
    append!(command, pairs(members_kw)...)
    execute(command, client)
end

function xautoclaim(name::AbstractString, groupname::AbstractString, consumername::AbstractString, min_idle_time::Union{Integer,Nothing}=nothing, start_id::Union{AbstractString,Nothing}=nothing, end_id::Union{AbstractString,Nothing}=nothing, count::Union{Integer,Nothing}=nothing, justid::Bool=false, client=get_global_client())
    command = ["XAUTOCLAIM", name, groupname, consumername]
    if min_idle_time !== nothing
        push!(command, "MINIDLETIME", @sprintf("%i", min_idle_time))
    end
    if start_id !== nothing
        push!(command, "START", start_id)
    end
    if end_id !== nothing
        push!(command, "END", end_id)
    end
    if count !== nothing
        push!(command, "COUNT", @sprintf("%i", count))
    end
    if justid
        push!(command, "JUSTID")
    end
    execute(command, client)
end


function xclaim(key::AbstractString, groupname::AbstractString, consumername::AbstractString, min_idle_time::Integer, ids::Union{AbstractString,Vector{<:AbstractString}}, idle::Union{Integer,Nothing}=nothing, time::Union{Integer,Nothing}=nothing, retrycount::Union{Integer,Nothing}=nothing, force::Bool=false, justid::Bool=false, client=get_global_client())
    command = ["XCLAIM", key, groupname, consumername, @sprintf("%i", min_idle_time)]
    if idle !== nothing
        push!(command, "IDLE", @sprintf("%i", idle))
    end
    if time !== nothing
        push!(command, "TIME", @sprintf("%i", time))
    end
    if retrycount !== nothing
        push!(command, "RETRYCOUNT", @sprintf("%i", retrycount))
    end
    if force
        push!(command, "FORCE")
    end
    if justid
        push!(command, "JUSTID")
    end
    append!(command, ids)
    execute(command, client)
end

function xdel(name::AbstractString, ids::Union{AbstractString,Vector{<:AbstractString}}, client=get_global_client())
    execute(["XDEL", name, ids...], client)
end

function xgroup_create(key::AbstractString, groupname::AbstractString, id::AbstractString, mkstream::Bool=false, client=get_global_client())
    # TODO $ instead of ID
    command = ["XGROUP", "CREATE", key, groupname, id]
    if mkstream
        push!(command, "MKSTREAM")
    end
    execute(command, client)
end

function xgroup_delconsumer(key::AbstractString, groupname::AbstractString, consumername::AbstractString, client=get_global_client())
    execute(["XGROUP", "DELCONSUMER", key, groupname, consumername], client)
end

function xgroup_destroy(key::AbstractString, groupname::AbstractString, client=get_global_client())
    execute(["XGROUP", "DESTROY", key, groupname], client)
end

function xgroup_createconsumer(key::AbstractString, groupname::AbstractString, consumername::AbstractString, client=get_global_client())
    execute(["XGROUP", "CREATECONSUMER", key, groupname, consumername], client)
end

function xgroup_setid(key::AbstractString, groupname::AbstractString, id::AbstractString, entries_read::Union{Integer,Nothing}=nothing, client=get_global_client())
    # TODO $ instead of id?
    command = ["XGROUP", "SETID", key, groupname, id]
    if entries_read !== nothing
        push!(command, "ENTRIESREAD", @sprintf("%i", entries_read))
    end
    execute(command, client)
end

function xinfo_consumers(key::AbstractString, groupname::AbstractString, client=get_global_client())
    execute(["XINFO", "CONSUMERS", key, groupname], client)
end

function xinfo_groups(key::AbstractString, client=get_global_client())
    execute(["XINFO", "GROUPS", key], client)
end

function xinfo_stream(key::AbstractString, full::Bool=false, count::Union{Integer,Nothing}=nothing, client=get_global_client())
    command = ["XINFO", "STREAM", key]
    if full
        push!(command, "FULL")
    end
    # TODO inside full block?
    if count !== nothing
        push!(command, "COUNT", @sprintf("%i", count))
    end
    execute(command, client)
end

function xlen(key::AbstractString, client=get_global_client())
    execute(["XLEN", key], client)
end

function xpending(key::AbstractString, groupname::AbstractString, start_id::AbstractString="-", end_id::AbstractString="+", count::Union{Integer,Nothing}=nothing, idle::Union{Integer,Nothing}=nothing, consumername::Union{AbstractString,Nothing}=nothing, client=get_global_client())
    # TODO test without start_id and end_id
    command = ["XPENDING", key, groupname]
    if idle !== nothing
        push!(command, "IDLE", @sprintf("%i", idle))
    end
    push!(command, start_id, end_id)
    if count !== nothing
        push!(command, "COUNT", @sprintf("%i", count))
    end
    if consumername !== nothing
        push!(command, "CONSUMER", consumername)
    end
    execute(command, client)
end

function xrange(key::AbstractString; start_id::AbstractString="-", end_id::AbstractString="+", count::Union{Integer,Nothing}=nothing, client=get_global_client())
    command = ["XRANGE", key, start_id, end_id]
    if count !== nothing
        push!(command, "COUNT", @sprintf("%i", count))
    end
    execute(command, client)
end


function xread(stream_ids::Union{AbstractDict,NamedTuple}, count::Union{Integer,Nothing}=nothing, block::Union{Integer,Nothing}=nothing, client=get_global_client())
    command::Vector{Any} = ["XREAD"]
    if count !== nothing
        push!(command, "COUNT", @sprintf("%i", count))
    end
    if block !== nothing
        push!(command, "BLOCK", @sprintf("%i", block))
    end
    push!(command, "STREAMS")
    append!(command, Base.keys(stream_ids))
    append!(command, values(stream_ids))
    execute(command, client)
end

function xreadgroup(groupname::AbstractString, consumername::AbstractString, stream_ids::Union{AbstractDict,NamedTuple}, count::Union{Integer,Nothing}=nothing, block::Union{Integer,Nothing}=nothing, noack::Bool=false, client=get_global_client())
    command = ["XREADGROUP", "GROUP", groupname, consumername]
    if count !== nothing
        push!(command, "COUNT", @sprintf("%i", count))
    end
    if block !== nothing
        push!(command, "BLOCK", @sprintf("%i", block))
    end
    if noack
        push!(command, "NOACK")
    end
    push!(command, "STREAMS")
    append!(command, Base.keys(stream_ids))
    append!(command, values(stream_ids))
    execute(command, client)
end

function xrevrange(key::AbstractString, end_id::AbstractString="+", start_id::AbstractString="-", count::Union{Integer,Nothing}=nothing, client=get_global_client())
    command = ["XREVRANGE", key, end_id, start_id]
    if count !== nothing
        push!(command, "COUNT", @sprintf("%i", count))
    end
    execute(command, client)
end

function xtrim(key::AbstractString; maxlen::Union{Integer,Nothing}=nothing, minid::Union{AbstractString,Nothing}=nothing, approximate::Bool=false, limit::Union{Integer,Nothing}=nothing, client=get_global_client())
    if maxlen !== nothing && minid !== nothing
        throw(ArgumentError("`maxlen` and `minid` cannot be specified together."))
    end
    if maxlen === nothing && minid === nothing
        throw(ArgumentError("Either `maxlen` or `minid` must be specified."))
    end
    command = ["XTRIM", key]
    if maxlen !== nothing
        push!(command, "MAXLEN")
    else
        push!(command, "MINID")
    end
    if approximate
        push!(command, "~")
    end
    if maxlen !== nothing
        push!(command, @sprintf("%i", maxlen))
    else
        push!(command, minid)
    end
    if limit !== nothing
        push!(command, "LIMIT", @sprintf("%i", limit))
    end
    execute(command, client)
end