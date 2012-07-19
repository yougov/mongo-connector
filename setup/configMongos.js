//(C) Copyright 2012, 10gen

//=========================================#
//         Configure Mongos                #
//=========================================#

db._adminCommand({addShard: "demo-repl/localhost:27117"});
//printjson(db._adminCommand({enablesharding: "demo-repl/localhost:27117/test"}));
