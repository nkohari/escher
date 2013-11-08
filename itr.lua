leveldb = require("leveldb")

db = leveldb.open("test.db", {create_if_missing=true})
iter = db:iterator()
iter:first()

while true do
  key, value = iter:next()
  if not key then break end
  print(key .. " = " .. value)
end
