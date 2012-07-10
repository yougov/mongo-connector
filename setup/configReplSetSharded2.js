//(C) Copyright 2012, 10gen

//=========================================#
//         Configure Replica Set           #
//=========================================#

config	= {_id: "demo-repl-2", members: [{_id : 0, host: "localhost:27317" },
				       {_id : 1, host: "localhost:27318" },
				       {_id : 2, host: "localhost:27319" , arbiterOnly: true}]};
rs.initiate(config);
while (!(rs.status().myState)){
    }
