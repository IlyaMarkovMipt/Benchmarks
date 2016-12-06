box.cfg{listen=3301, slab_alloc_factor=1.17, slab_alloc_minimal=50}
os.execute('rm -rf 51* 0* tarantool.log')
engine = 'memtx'
box.schema.user.grant('guest', 'execute,read,write', 'universe', nil, {if_not_exists = true})
demo = box.schema.space.create('demo', {id = 513, if_not_exists = true, engine=engine})
demo:create_index('primary', {id = 0, unique = true, type = "tree", parts={1, 'string'}, if_not_exists = true})

sql = box.schema.space.create('sql', {id = 514, if_not_exists = true, engine=engine})
sql:create_index('primary', {id = 0, unique = true, type = "tree", parts={1, 'number'}, if_not_exists = true})
console=require('console')


digest = require('digest')

function insert_hash(str, coords)
    local hash = digest.sha1(str)
    demo:insert{hash,coords}
end


function my_get(str)
    local hash = digest.sha1(str)
    local sample = demo:select{hash}
    if #sample == 1 then
        return {str, sample[1][2]}
    else
        print 'bad hash'
    end
end

function mem_used(id)
    local size = 0
    s = box.space[id]
    size = s.index.primary:bsize()
    local tuples = s:select{}
    for _,t in pairs(tuples)
        do
            size = size + t:bsize()
        end
    return size / (1024*1024)
end

console.start()
