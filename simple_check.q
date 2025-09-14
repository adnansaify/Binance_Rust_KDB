/ Start KDB+ server on port 5001
\p 5001

/ Simple database setup
system "mkdir -p ./hdb"

/ Real-time table (in memory)
tickers:([] time:`timestamp$(); date:`date$(); symbol:`symbol$(); close:`float$(); open:`float$(); high:`float$(); low:`float$(); volume:`float$())

/ Historical data storage
allTickers:([] time:`timestamp$(); date:`date$(); symbol:`symbol$(); close:`float$(); open:`float$(); high:`float$(); low:`float$(); volume:`float$())

/ Load existing data if available
if[`allTickers.dat in key `:./hdb/; allTickers::get `:./hdb/allTickers.dat]

/ Function to add ticker data
addTicker:{[sym;c;o;h;l;v]
    currentTime:.z.p;
    currentDate:.z.d;
    
    / Insert into real-time table
    `tickers insert (currentTime; currentDate; sym; c; o; h; l; v);
    
    / Save every 50 records
    if[0=(count tickers) mod 50;
        -1 "Reached ", string count tickers, " records - saving...";
        saveToHDB[];
    ];
    
    -1 "=== BTCUSDT TICKER ===";
    -1 "Symbol: ", string sym;
    -1 "Close:  $", string c;
    -1 "Open:   $", string o; 
    -1 "High:   $", string h;
    -1 "Low:    $", string l;
    -1 "Volume: ", string v;
    -1 "RDB Records: ", string count tickers;
    -1 "HDB Records: ", string count allTickers;
    -1 "=====================";
    }

/ Function to save to Historical Database (SIMPLIFIED)
saveToHDB:{[]
    if[0<count tickers;
        / Append RDB data to HDB
        `allTickers insert select from tickers;
        
        / Save to disk
        `:./hdb/allTickers.dat set allTickers;
        
        -1 "Saved ", string count tickers, " records to HDB";
        -1 "Total HDB records: ", string count allTickers;
        
        / Clear real-time table
        delete from `tickers;
        -1 "RDB cleared";
    ];
    }

/ Function to get all data
getAllData:{[] allTickers,tickers}

/ Function to get recent data
getRecent:{[n] (neg n)#getAllData[]}

/ Function to get stats
getStats:{[]
    allData:getAllData[];
    -1 "=== DATABASE STATS ===";
    -1 "Total Records: ", string count allData;
    -1 "RDB Records: ", string count tickers;
    -1 "HDB Records: ", string count allTickers;
    if[count allData;
        -1 "Latest Close: $", string last allData.close;
        -1 "Min Price: $", string min allData.close;
        -1 "Max Price: $", string max allData.close;
        -1 "Avg Volume: ", string avg allData.volume;
    ];
    -1 "Database File: ./hdb/allTickers.dat";
    -1 "=====================";
    }

/ Save on exit
.z.exit:{saveToHDB[]; -1 "Database saved on exit"}

.z.ps:{[q] value q}
.z.po:{[h] -1 "Connected: ", string h}

-1 "KDB Database ready on port 5001"
-1 "Database: ./hdb/allTickers.dat"
-1 "Commands: getAllData[], getStats[], getRecent[n]"
-1 "Auto-save every 50 records"
if[count allTickers; -1 "Loaded ", string count allTickers, " existing records"]