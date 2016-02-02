# skynet_memcached
memcached client for skynet


useage:


local memcached = require("memcached").new()
memcached:connect()



local func = function (  )
	print("===============")
	print("version=",memcached:version())
	dump(memcached:stats()," stats=")
	memcached:flush_all()
	memcached:set("hi","you are welcome")
	memcached:set("hello","world")
	dump(memcached:gets({"hi","hello"})," multi gets==")
	
	memcached:append("hello"," append")
	local ret = memcached:get({"hi","hello"})
	dump(ret," myret=")
	memcached:set("num",1)
	print("incr num = ",memcached:incr("num",2))
	print("decr num = ",memcached:decr("num",1))
	print("delete = ",memcached:delete("num"))
	print("get num = ",memcached:get("num"))
	dump(memcached:gets("hi")," gets hi==")
	dump(memcached:gets("hello")," gets hello==")

	
	-- memcached:quit()
end
-- skynet.fork(func)

func()