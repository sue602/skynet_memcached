-- Copyright (C) 2012-2013 Yichun Zhang (agentzh), CloudFlare Inc.

local socketchannel = require "socketchannel"
local mysocketchannel = nil

local sub = string.sub
local gsub = string.gsub
local escape_uri = function (s)
    return (gsub(s, "([^A-Za-z0-9_])", function(c)
        return string.format("%%%02X", string.byte(c))
    end))
end

local unescape_uri = function (s)
    s = gsub(s, "+", " ")
    s = gsub(s, "%%(%x%x)", function (h)
      return string.char(tonumber(h, 16))
    end)
    return s
end

local match = string.match
local strlen = string.len
local concat = table.concat
local setmetatable = setmetatable
local type = type
local error = error


local _M = {
    _VERSION = '0.13'
}

local DEFAULT_HOST = "127.0.0.1"
local DEFAULT_PORT = 11211

local mt = { __index = _M }


function _M.new(self, opts)
    local escape_key = escape_uri
    local unescape_key = unescape_uri

    if opts then
       local key_transform = opts.key_transform

       if key_transform then
          escape_key = key_transform[1]
          unescape_key = key_transform[2]
          if not escape_key or not unescape_key then
             return nil, "expecting key_transform = { escape, unescape } table"
          end
       end
    end

    return setmetatable({
        -- sock = sock,
        escape_key = escape_key,
        unescape_key = unescape_key,
    }, mt)
end

--[[
function _M.set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end
]]


function _M.connect(self, ...)
    local host,port = ...
    local channel = socketchannel.channel {
        host = host or DEFAULT_HOST,
        port = port or DEFAULT_PORT,
        nodelay = true,
    }
    -- try connect first only once
    channel:connect(true)
    mysocketchannel = channel
end

local function myreadreply( sock ,flag )
    -- print("  flag=",flag)
    if "quit" == flag then
        return true,1
    end
    local res = sock:readline("\r\n")
    -- print("res = ",res,"  flag=",flag)
    if not res then
        return false, "err read *l"
    end

    if "set" == flag or 
       "add" == flag or 
       "replace" == flag or 
       "append" == flag or 
       "prepend" == flag then

        if res == "STORED" then
            return true,1
        end
        return nil, res
    ----------------------------------
    elseif "get" == flag then
        if res == 'END' then
            return true, nil --返回nil
        end

        local flags, len = match(res, '^VALUE %S+ (%d+) (%d+)$')
        if not flags then
            return nil,("bad line: " .. res)
        end

        -- print("len: ", len, ", flags: ", flags)

        local data = sock:read(len)
        if not data then
            return nil,data
        end

        local line = sock:read(7) -- discard the trailing "\r\nEND\r\n"
        if not line then
            return nil, ("failed to receive value trailer: " .. line)
        end

        return true, {data,flags}
    ----------------------------------
    elseif "gets" == flag then
        if res == 'END' then
            return true, {nil,nil,res}
        end

        local flags, len, cas_uniq = match(res, '^VALUE %S+ (%d+) (%d+) (%d+)$')
        if not flags then
            return true,{nil,nil,res} --返回nil
        end

        -- print("len: ", len, ", flags: ", flags)

        local data = sock:read(len)
        if not data then
            return nil, {nil, nil, "gets err"}
        end

        local line= sock:read(7) -- discard the trailing "\r\nEND\r\n"
        if not line then
            return nil, {nil, nil, "gets read 7 err"}
        end

        return true,{data, flags, cas_uniq}
    ----------------------------------
    elseif "multi_get" == flag then
        local results = {}

        while true do

            if res == 'END' then
                break
            end

            local key, flags, len = match(res, '^VALUE (%S+) (%d+) (%d+)$')
            -- print("key: ", key, "len: ", len, ", flags: ", flags)

            if key then
                local data = sock:read(len)
                if not data then
                    return nil, ("multi_get error =" .. tostring(len))
                end

                results[unescape_uri(key)] = {data, flags}

                data = sock:read(2) -- discard the trailing CRLF
                if not data then
                    return nil, "err"
                end
            end

            res = sock:readline("\r\n")
        end

        return true,results
    ----------------------------------
    elseif "multi_gets" == flag then
        local results = {}
        while true do
            -- print("res ====== ",res)
            if res == 'END' then
                break
            end

            local key, flags, len, cas_uniq = match(res, '^VALUE (%S+) (%d+) (%d+) (%d+)$')
            -- print("key: ", key, "len: ", len, ", flags: ", flags,", cas_uniq: ",cas_uniq)

            if key then
                -- print("read len = ",len)
                local data = sock:read(len)
                if not data then
                    return nil, ("multi_gets error =" .. tostring(len))
                end
                -- print("data ==",data)
                results[unescape_uri(key)] = {data, flags, cas_uniq}

                data = sock:read(2) -- discard the trailing CRLF
                if not data then
                    return nil, "err"
                end
            end

            res = sock:readline("\r\n")
        end
        -- print(" ok ========")
        return true,results
    ----------------------------------
    elseif "touch" == flag then
        -- moxi server from couchbase returned stored after touching
        if res == "TOUCHED" or res =="STORED" then
            return true,1
        end
        return nil, res
    ----------------------------------
    elseif "verbosity" == flag or "flush_all" == flag then
        if res ~= 'OK' then
            return nil, res
        end
        return true,1
    ----------------------------------
    elseif "version" == flag then
        local ver = match(res, "^VERSION (.+)$")
        if not ver then
            return nil, ver
        end
        return true,ver
    ----------------------------------
    elseif "stats" == flag then
        local lines = {}
        local n = 0
        while true do
            if res == 'END' then
                return true,lines
            end

            if not match(res, "ERROR") then
                n = n + 1
                lines[n] = res
            else
                return nil, res
            end
            res = sock:readline("\r\n")
        end
        -- cannot reach here...
        return true,lines
    ----------------------------------
    elseif "incr" == flag or "decr" == flag then
        if not res then
            return nil, "incr or decr err"
        end
        local tmpmatch = match(res, '^%d+$')
        if not tmpmatch then
            return true, tmpmatch --返回 nil
        end
        return true,res
    ----------------------------------
    elseif "delete" == flag then
        if res ~= 'DELETED' then
            return nil, res
        end

        return true,1
    elseif "cas" == flag then
        if res == "STORED" then
            return true,1
        end
        return true, res
    end

    return true,res
end


local function _query_resp(flag)
     -- print("_query_resp == ",flag)
     return function(sock)
        return myreadreply(sock,flag)
    end
end

local function _multi_get(self, keys)

    local nkeys = #keys

    if nkeys == 0 then
        return {}, nil
    end

    local escape_key = self.escape_key
    local cmd = {"get"}
    local n = 1

    for i = 1, nkeys do
        cmd[n + 1] = " "
        cmd[n + 2] = escape_key(keys[i])
        n = n + 2
    end
    cmd[n + 1] = "\r\n"
    -- dump(cmd," cmds=")
    -- print("multi get cmd: ", cmd)

    local query_resp = _query_resp("multi_get")
    return mysocketchannel:request(concat(cmd), query_resp )
end


function _M.get(self, key)
    if type(key) == "table" then
        return _multi_get(self, key)
    end

    local req = "get " .. self.escape_key(key) .. "\r\n"
    local query_resp = _query_resp("get")
    return mysocketchannel:request(req, query_resp )
end


local function _multi_gets(self, keys)
    local nkeys = #keys

    if nkeys == 0 then
        return {}, nil
    end

    local escape_key = self.escape_key
    local cmd = {"gets"}
    local n = 1
    for i = 1, nkeys do
        cmd[n + 1] = " "
        cmd[n + 2] = escape_key(keys[i])
        n = n + 2
    end
    cmd[n + 1] = "\r\n"

    -- print("multi get cmd: ", cmd)

    local query_resp = _query_resp("multi_gets")
    return mysocketchannel:request(concat(cmd), query_resp )
end


function _M.gets(self, key)
    if type(key) == "table" then
        return _multi_gets(self, key)
    end
    local req = "gets " .. self.escape_key(key) .. "\r\n"
    local query_resp = _query_resp("gets")
    return mysocketchannel:request(req, query_resp )
end


local function _expand_table(value)
    local segs = {}
    local nelems = #value
    local nsegs = 0
    for i = 1, nelems do
        local seg = value[i]
        nsegs = nsegs + 1
        if type(seg) == "table" then
            segs[nsegs] = _expand_table(seg)
        else
            segs[nsegs] = seg
        end
    end
    return concat(segs)
end


local function _store(self, cmd, key, value, exptime, flags)
    if not exptime then
        exptime = 0
    end

    if not flags then
        flags = 0
    end

    if type(value) == "table" then
        value = _expand_table(value)
    end

    local req = cmd .. " " .. self.escape_key(key) .. " " .. flags .. " "
                .. exptime .. " " .. strlen(value) .. "\r\n" .. value
                .. "\r\n"

    local query_resp = _query_resp(cmd)
    return mysocketchannel:request(req, query_resp )
end


function _M.set(self, ...)
    return _store(self, "set", ...)
end


function _M.add(self, ...)
    return _store(self, "add", ...)
end


function _M.replace(self, ...)
    return _store(self, "replace", ...)
end


function _M.append(self, ...)
    return _store(self, "append", ...)
end


function _M.prepend(self, ...)
    return _store(self, "prepend", ...)
end


function _M.cas(self, key, value, cas_uniq, exptime, flags)
    if not exptime then
        exptime = 0
    end

    if not flags then
        flags = 0
    end

    local req = "cas " .. self.escape_key(key) .. " " .. flags .. " "
                .. exptime .. " " .. strlen(value) .. " " .. cas_uniq
                .. "\r\n" .. value .. "\r\n"

    local query_resp = _query_resp("cas")
    return mysocketchannel:request(req, query_resp )
end


function _M.delete(self, key)
    key = self.escape_key(key)

    local req = "delete " .. key .. "\r\n"

    local query_resp = _query_resp("delete")
    return mysocketchannel:request(req, query_resp )
end

--[[
function _M.set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end


function _M.get_reused_times(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end
]]

function _M.flush_all(self, time)
    local req
    if time then
        req = "flush_all " .. time .. "\r\n"
    else
        req = "flush_all\r\n"
    end

    local query_resp = _query_resp("flush_all")
    return mysocketchannel:request(req, query_resp )
end


local function _incr_decr(self, cmd, key, value)
    local req = cmd .. " " .. self.escape_key(key) .. " " .. value .. "\r\n"
    local query_resp = _query_resp(cmd)
    return mysocketchannel:request(req, query_resp )
end


function _M.incr(self, key, value)
    return _incr_decr(self, "incr", key, value)
end


function _M.decr(self, key, value)
    return _incr_decr(self, "decr", key, value)
end


function _M.stats(self, args)
    local req
    if args then
        req = "stats " .. args .. "\r\n"
    else
        req = "stats\r\n"
    end

    local query_resp = _query_resp("stats")
    return mysocketchannel:request(req, query_resp )
end


function _M.version(self)
    local req = "version\r\n"
    local query_resp = _query_resp("version")
    return mysocketchannel:request(req, query_resp )
end


function _M.quit(self)
    local req = "quit\r\n"
    local query_resp = _query_resp("quit")
    local ret =  mysocketchannel:request(req, query_resp )
    if 1 == ret then
        self:close()
    end
end


function _M.verbosity(self, level)
    local req = "verbosity " .. level .. "\r\n"
    local query_resp = _query_resp("verbosity")
    return mysocketchannel:request(req, query_resp )
end


function _M.touch(self, key, exptime)
    local req = "touch " .. self.escape_key(key) .. " " .. exptime .. "\r\n"
    local query_resp = _query_resp("touch")
    return mysocketchannel:request(req, query_resp )
end


function _M.close(self)
    mysocketchannel:close()
    mysocketchannel = nil
end


return _M
