//(C) Copyright 2012, 10gen

//=========================================#
//         Configure Mongos                #
//=========================================#

printjson(db._adminCommand({addShard: "demo-repl/localhost:27117"}));
printjson(db._adminCommand({addShard: "demo-repl-2/localhost:27317"}));

