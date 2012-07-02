//(C) Copyright 2012, 10gen

//=========================================#
//         Configure Mongos                #
//=========================================#
db._adminCommand({addShard: "demo-repl/localhost:27117"});
db._adminCommand({enablesharding: "test"});

