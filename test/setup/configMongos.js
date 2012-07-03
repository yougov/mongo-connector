//(C) Copyright 2012, 10gen

//=========================================#
//         Configure Mongos                #
//=========================================#

print("starting");
printjson(db._adminCommand({addShard: "demo-repl/localhost:27117"}));
print("finished first command");
//printjson(db._adminCommand({enablesharding: "demo-repl/localhost:27117/test"}));
print("ending");
