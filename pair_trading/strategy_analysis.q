//get signals that occur within 5 seconds of each other by ticker
df_streaks: update streakCount:1+1{y*x+y}\00:00:05>deltas time by tickers from `tickers`time xasc update time:date+time from select from  df where (abs(e)) > abs((sqrt Q)), not date=2022.01.04, not time within(09:30:00;09:35:00); 
df_streaks: `tickers`time xasc select from df_streaks where streakCount<> next streakCount, y_est<>0;  /error in computation y_est is 0

//get a table containg sym and the date the sym has a trading signal (1 date per row)
dates: update n: count raze date by sym from (select distinct date by sym from update sym: {first `$"&" vs string x} each tickers, 
    sym: {last `$"&" vs string x} each tickers from df_streaks), select distinct date by sym from update sym1: {first `$"&" vs string x} each tickers, sym: {last `$"&" vs string x} each tickers from df_streaks;
dates: distinct flip `sym`date!(raze{(dates[x]`n)#x} each exec sym from dates;raze exec date from dates);

//create a table containg the date a signal occurs for a given sym, and the intraday time of such signals
flags: `tickers`date`min_time`max_time`sym`sym2 xcol (select min_time: min `second$time, max_time: max `second$time by tickers, date from df_streaks) lj `tickers xkey distinct select tickers, sym1, sym2 from update sym1: {first `$"&" vs string x} each tickers, sym2: {last `$"&" vs string x} each tickers from df_streaks;
flags: distinct delete tickers from (,/) (0!delete sym2 from flags;0!`tickers`date`min_time`max_time`sym xcol delete sym from flags);

//Get Orderbook Depth by sym, second, and date to compare depth when a signal occurs vs when it doesn't

\\Constant Values order data
input.symbols : distinct raze exec sym from dates;
input.startTime : 09:30:00.000;
input.endTime :  16:00:00.000 
input.columnsO: `eventTimestamp`instrumentID`listing_mkt`event`b_po`b_type`b_sme`s_po`s_type`s_sme`price`volume; 
input.applyFilter : (in;`listing_mkt;enlist(`TSE`AQL));



order_results: flip `instrumentID`listing_mkt`time`date`bid_depth`b_n_orders`ask_depth`a_n_orders`flag !(`symbol$();`symbol$();`second$();`date$();`float$();`long$();`float$();`long$();`symbol$());

.mapq.summarystats.filterorders:{[OO]
// Order-based filters
    OO: eval (!;0;(?;OO;enlist((in;`event;enlist`Order);(>;`price;0);(>;`volume;0));0b;()));
    : @[;`instrumentID;`p#] `instrumentID xasc OO;
    }; 

mm_orders:{[t] 
    t: 0^(select bid_depth: sum volume*price, b_n_orders: count i by instrumentID, listing_mkt, time: 1 xbar `second$eventTimestamp, 
        date: `date$eventTimestamp from t where b_po<>0,b_type<>`) lj select ask_depth: sum volume*price, a_n_orders: count i by instrumentID, listing_mkt, 
    time: 1 xbar `second$eventTimestamp, date: `date$eventTimestamp from t where s_po<>0,s_type<>`;
    :delete min_time, max_time from update flag:?[time within(min_time;max_time); `Y;`] from t lj `date`instrumentID  xcol `date`sym xkey flags;
    };

calendar: distinct raze exec date from dates;
i: 1;
while[i<count[calendar];
    input.startDate: calendar[i];
    input.endDate: input.startDate;
    
    //Get Order Data
    getData.edwO: `..getTicks[`symList`assetClass`dataType`startDate`endDate`startTime`endTime`idType`columns`applyFilter!(input.symbols;`equity;`order;input.startDate;input.endDate;input.startTime;input.endTime;`instrumentID;input.columnsO;input.applyFilter)]; /13.6m 
    
    
    //Filter Order Data
    orders: .mapq.summarystats.filterorders getData.edwO;
    
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `getData.edwO; /delete all records for tables in memory
    
    
    //Join Summary Stats and Append Results to empty table
    order_results,: 0!mm_orders orders;
    
    
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `orders; /delete all records for tables in memory

    //Iterate again
    i+: 1;
    
    ];
