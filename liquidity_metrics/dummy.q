GetInputDates: {[input.start.date; input.end.date]
    dates: {[n] {x+2000.01.01}each n}[til (.z.d-2000.01.01)]; /get all days til yesterday
    calendar: desc raze (`mon;`tue;`wed;`thur;`fri)!(dates where ((5+til count dates) mod 7)= 0;dates where ((4+til count dates) mod 7)= 0;dates where ((3+til count dates) mod 7)= 0;dates where ((2+til count dates) mod 7)= 0;dates where ((1+til count dates) mod 7)= 0);
    calendar: calendar where calendar <= input.end.date;
    :0N 280#calendar 1 + til calendar?last calendar where calendar>=input.start.date;
    };
calendar: GetInputDates[2019.01.01;2024.01.01];
 
/
Calculate Daily Liquidity Metrics
\
//Constant Values
input.symbols :`;
input.startTimeT : 09:30:00.000;
input.startTimeQ : 09:25:00.000;  
input.endTime :  10:30:00.000;
input.columnsT :   `sym`listing_mkt`time`volume`total_value`s_type`s_short`s_short_marking_exempt`b_short_marking_exempt`s_market_maker`b_market_maker`s_program_trade`b_user_name`s_user_name /`sym`listing_mkt`time`mkt`volume`price`total_value`event`b_type`s_type`b_market_maker`s_market_maker`b_active_passive`s_active_passive`trade_stat`trade_correction`s_dark`b_dark`s_internal_cross`mkt`s_short`s_short_marking_exempt`b_short_marking_exempt`s_user_name`b_user_name`s_program_trade;
input.columnsQ : `sym`listing_mkt`mkt`time`nat_best_bid`nat_best_offer`nat_best_bid_size`nat_best_offer_size`ask_price`bid_price`ask_size`bid_size;
input.tableQ : `quote;
input.tableT : `trade;
input.applyFilterT :  ((=;`event;enlist`Trade);(<>;`trade_stat;enlist`C);(=;`trade_correction;"N");(>;`volume;0);(in;`listing_mkt;enlist`AQL)); 
input.applyFilterQ : ((in;`listing_mkt;enlist`AQL);(>;`ask_price;`bid_price);(<;`ask_price;0W);(>;`bid_price;0);(>;`nat_best_offer;`nat_best_bid);(within;`nat_best_offer;(enlist;(%;`nat_best_bid;3);(*;3;`nat_best_bid))));
       
//Create empty table to store results
output.cols: `date`mkt`sym`listing_mkt`maxbid`min_ask`last_bid`last_ask`last_mid_price`total_volume`total_value`vwap`adv`range`maxprice`minprice`last_price`num_of_trades`num_of_block_trades`block_volume`block_value`dark_volume`dark_value`num_of_dark_trades`short_volume`total_short_value`num_of_short_trades`dqs`pqs`num_quotes`des_k`pes_k`wmid`volumebuy`volumesell`orderbookimbalance`realized_vol`drs_k_1m`prs_k_1m`drs_k_5m`prs_k_5m`dpi_k_1m`ppi_k_1m`dpi_k_5m`ppi_k_5m;
dailyliqmet: flip (output.cols)!(`date$();`symbol$();`symbol$();`symbol$();`float$();`float$();`float$();`float$();`float$();`long$();`float$();`float$();`float$();`float$();`float$();`float$();`float$();`long$();`long$();`long$();`float$();`long$();`float$();`long$();`long$();`float$();`long$();`float$();`float$();`long$();`float$();`float$();`float$();`long$();`long$();`float$();`float$();`float$();`float$();`float$();`float$();`float$();`float$();`float$();`float$());
 
hhi: flip `sym`listing_mkt`date`hhi_volume!(`symbol$();`symbol$();`symbol$();`float$())


t: select from getData.edwT where ({x in 100#x};i) fby sym
q: select from getData.edwQ where ({x in 100#x};i) fby sym


//Inititate while loop
i:0;
while[i<count[calendar];
    //Get Pairs in right order for Kalman Filter
    input.startDate:  2023.12.28 /last calendar[i];
    input.endDate:   2023.12.29 /first calendar[i];
    
    //Get Trade Data
    getData.edwT: @[;`sym;`p#] `sym`time xasc update date: `date$time from `..getTicks[`assetClass`dataType`symList`startDate`endDate`startTime`endTime`columns`applyFilter!(`equity;
        input.tableT;
        input.symbols;
        input.startDate;input.endDate;
        input.startTimeT;input.endTime;
        input.columnsT;
        input.applyFilterT)]; 
    
    
    //Get Quote Data
    getData.edwQ: @[;`sym;`p#] `sym`time xasc update date: `date$time, mid: .5* nat_best_bid+nat_best_offer from `..getTicks[`assetClass`dataType`symList`startDate`endDate`startTime`endTime`columns`applyFilter!(`equity;
        input.tableQ;
        input.symbols;
        input.startDate;input.endDate;
        input.startTimeQ;input.endTime;
        input.columnsQ;
        input.applyFilterQ)];
    /short_trades
    //Merge Trade and Quote data
    TradesnQuotes: .mapq.summarystats.tradesnquotes[getData.edwT;getData.edwQ]
    
    //Execute Market Quality Functions
    
    summarystatsquotes_natbidask: .mapq.summarystats.summarystatsquotes[Quotes; `nat_best_bid`nat_best_offer; input.startTime;input.endTime]; //summary stats quotes
   
    summarystats_trades: .mapq.summarystats.summarystatstrades[Trades; input.startTime;input.endTime]; //summary stats trades
    
 
    qs_natbidask: .mapq.summarystats.qs[getData.edwQ;`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //Quoted Spreads
    
    es: .mapq.summarystats.es[.mapq.summarystats.tradesnquotes[Trades;Quotes];`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //Effective Spreads
    
    short_trades: .mapq.summarystats.shorttrades[input.Trades;input.startTime;input.endTime]; //Short Trade Statistics
    
    orderbook_depth: .mapq.summarystats.midorderbook[Quotes;`nat_best_bid`nat_best_offer`nat_best_bid_size`nat_best_offer_size;input.startTime;input.endTime]; //orderbook depth
    
    rs_1m: `date`mkt`sym`listing_mkt`drs_k_1m`prs_k_1m xcol .mapq.summarystats.rs[Trades;Quotes;00:01:00.000;`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //realized spreads
    rs_5m: `date`mkt`sym`listing_mkt`drs_k_5m`prs_k_5m xcol .mapq.summarystats.rs[Trades;Quotes;00:05:00.000;`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //realized spreads

    pi_1m: `date`mkt`sym`listing_mkt`dpi_k_1m`ppi_k_1m xcol .mapq.summarystats.pi[Trades;Quotes;00:01:00.000;`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //price impact
    pi_5m: `date`mkt`sym`listing_mkt`dpi_k_5m`ppi_k_5m xcol .mapq.summarystats.pi[Trades;Quotes;00:05:00.000;`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //price impact
    
    orderbook_imbalance: .mapq.summarystats.orderbookimbalance[Trades; input.startTime; input.endTime]; //orderbook_imbalance
    
    r_volatitlity: .mapq.summarystats.realizedvolatility[Trades;input.startTime;input.endTime]; //realized volatility
    
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `getData.edwT`getData.edwQ`TradesnQuotes; /delete all records for tables in memory
    
    //Join Liquidity metrics and Append Results to empty table
    dailyliqmet,: 0!(uj/)(summarystatsquotes_natbidask;summarystats_trades;short_trades;twap_natbidask;qs_natbidask;es;orderbook_depth;orderbook_imbalance;r_volatitlity;rs_1m;rs_5m;pi_1m;pi_5m);
    
    //Sleep 5 munites to bypass timeout
    /{t:.z.p;while[.z.p<t+x]} 00:05:00;  

    //Iterate again
    i+: 1;
    ];

 select count distinct sym by `date$time from getData.edwT
     hhi,: 0! select hhi_volume: sum mkt_share by sym, listing_mkt, date: `$ssr[;".";"-"] each string date from update mkt_share: (volume%daily_volume) xexp 2 from (`sym`listing_mkt xasc 0! select daily_volume: sum volume by sym, listing_mkt, date: `date$time from getData.edwT) lj `sym`listing_mkt`date xkey select sym, listing_mkt, date: `date$time, mkt, volume from getData.edwT;
     hhi: distinct hhi;
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `getData.edwT;


//Merge trade and quotes by same instant quotes, or a priori-quotes with a 10 second maximum delta
.mapq.summarystats.tradesnquotes:{[input.trades;input.quotes]
    t:aj[`sym`time;getData.edwT;update time_quote: time from getData.edwQ];
    :@[;`sym;`p#] `sym`time xasc (`time_quote`b_active_passive`s_active_passive`trade_stat`trade_correction)_select from 
    (update date: `date$time, lag_bw_q_t:time-time_quote, mid: 0.5*nat_best_bid+nat_best_offer, 
    tick: ?[b_active_passive=s_active_passive;?[(price>0.5*nat_best_bid+nat_best_offer) or 
                (price=0.5*nat_best_bid+nat_best_offer and price > prev price and sym = prev sym);1;-1];?[b_active_passive="A";1;-1]] from t) 
    where 0D00:00:10.000 > time - time_quote, time_quote<>0Np;};


//Time-Weighted Quoted Spreads (by Timeframe)
.mapq.summarystats.qs:{[input.table; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table; /rename bid and ask columns to bid_price and ask_price
    :$[`mkt in cols input.table;
        select dqs: ((next time) - time) wavg (ask- bid), pqs: ((next time) - time) wavg ((ask- bid)%(0.5*ask+bid)), num_quotes: count i by date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time); 
        select dqs: ((next time) - time) wavg (ask- bid), pqs: ((next time) - time) wavg ((ask- bid)%(0.5*ask+bid)), num_quotes: count i by date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };

///Volume-Weighted Effective Spreads
.mapq.summarystats.es:{[input.table; input.columns; input.start.time; input.end.time] 
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table; /rename bid and ask columns to bid_price and ask_price
    :$[`mkt in cols input.table; 
             select des_k: volume wavg (2*tick*(price-0.5*bid+ask)), pes_k:volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time); /calculate effective spread for kth trade and aggregate using volume weighted avg
             select des_k: volume wavg (2*tick*(price-0.5*bid+ask)), pes_k:volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };



//Intraday Orderbook Depth
.mapq.summarystats.midorderbook:{[input.table;input.columns;input.start.time; input.end.time] 
    /WEIGHTED MID FOR N LEVELS OF AN ORDER BOOK
    bid_price: input.columns where input.columns like "*bid_p*";nat_bid_price: input.columns where input.columns like "nat_best_bid"; ask_price: input.columns where input.columns like "*ask_p*";offer_price: input.columns where input.columns like "nat_best_offer"; /get column names for input bid and ask columns 
    bid_size: input.columns where  input.columns like "*bid_size*" ;ask_size: input.columns where input.columns like "*ask_size*"; offer_size: input.columns where input.columns like "*offer_size*";
    input.table: ((bid_price,nat_bid_price,ask_price,offer_price,bid_size,ask_size,offer_size)!`bid`ask`bid_size`ask_size)xcol input.table;
    :$[`mkt in cols input.table;
        select sdept: 0.5* sum (ask_size;bid_size), wmid: sum (bid_size; ask_size) wavg (bid; ask) by date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time);
        select ddep: 0.5* sum(bid*bid_size;ask*ask_size) sdept: 0.5* sum (ask_size;bid_size), wmid: sum (bid_size; ask_size) wavg (bid; ask) by date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };

//Volume-Weighted Realized Spreads
.mapq.summarystats.rs: {[input.trades;input.quotes;input.time; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns like "*bid_p*"; nat_bid_price: input.columns where input.columns like "nat_best_bid"; ask_price: input.columns where input.columns like "*ask_p*";offer_price: input.columns where input.columns like "nat_best_offer";
    input.quotes: ((bid_price,nat_bid_price,ask_price,offer_price)!`bid`ask)xcol input.quotes;
    ttnQQ: .mapq.summarystats.tradesnquotes[input.trades; update time: input.time +\: time from input.quotes];
    :$[`mkt in cols ttnQQ;
        select drs_k: volume wavg (2*tick*(price-0.5*bid+ask)), prs_k: volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by date, mkt, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time);
        select drs_k: volume wavg (2*tick*(price-0.5*bid+ask)), prs_k: volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by date, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time)]
   };

//Intraday Price Impact
.mapq.summarystats.pi: {[input.trades;input.quotes;input.time; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns like "*bid_p*"; nat_bid_price: input.columns where input.columns like "nat_best_bid"; ask_price: input.columns where input.columns like "*ask_p*";offer_price: input.columns where input.columns like "nat_best_offer";
    ttnQQ: .mapq.summarystats.tradesnquotes[input.trades;input.quotes];
    ttnQQ: ((bid_price,nat_bid_price,ask_price,offer_price)!`bid`ask)xcol ttnQQ;
    input.quotes_forward: update time: input.time +\: time from ((bid_price,nat_bid_price,ask_price,offer_price)!`bid_price`ask_price)xcol input.quotes;
    ttnQQ: .mapq.summarystats.tradesnquotes[ttnQQ; select sym, time, bid_price, ask_price from input.quotes_forward];
    :$[`mkt in cols ttnQQ;
        select dpi_k: volume wavg (2*tick*((0.5*bid_price+ask_price)-0.5*bid+ask)), ppi_k: volume wavg (2*tick*((0.5*bid_price+ask_price)-0.5*bid+ask))%(0.5*bid+ask) by date, mkt, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time);
        select dpi_k: volume wavg (2*tick*((0.5*bid_price+ask_price)-0.5*bid+ask)), ppi_k: volume wavg (2*tick*((0.5*bid_price+ask_price)-0.5*bid+ask))%(0.5*bid+ask) by date, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time)]
   };


//Intraday Price Improvement
.mapq.summarystats.priceimprovement: {[input.trades;input.quotes;input.time; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns like "*bid_p*"; nat_bid_price: input.columns where input.columns like "nat_best_bid"; ask_price: input.columns where input.columns like "*ask_p*";offer_price: input.columns where input.columns like "nat_best_offer";
    ttnQQ: .mapq.summarystats.tradesnquotes[input.trades;input.quotes];
    ttnQQ: ((bid_price,nat_bid_price,ask_price,offer_price)!`bid`ask)xcol ttnQQ;
    :$[`mkt in cols ttnQQ;
       select pi_b: volume wavg ?[tick=1; (offer-price)*volume;0Nj], pi_s: volume wavg ?[tick=-1; (price-bid)*volume;0Nj] by date, mkt, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time);
       select pi_b: volume wavg ?[tick=1; (offer-price)*volume;0Nj], pi_s: volume wavg ?[tick=-1; (price-bid)*volume;0Nj] by date, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time)]
   };


//OrderBook Imbalance
.mapq.summarystats.orderbookimbalance:{[input.trades; input.start.time; input.end.time]
    input.trades: $[`mkt in cols input.trades; 
             select volumebuy: sum raze ?[b_active_passive="A"; volume;0],  volumesell:  sum raze ?[s_active_passive="A"; volume;0] by date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time);
             select volumebuy: sum raze ?[b_active_passive="A"; volume;0],  volumesell:  sum raze ?[s_active_passive="A"; volume;0] by date, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)];
    :update orderbookimbalance: (volumebuy-volumesell)%volumebuy+volumesell from  input.trades;
    };


//Intraday Volatilty 1 min
.mapq.summarystats.realizedvolatility:{[input.trades; input.time; input.start.time; input.end.time]
    :$[`mkt in cols input.trades; 
        select realized_vol: dev realized_vol by date, mkt, sym, listing_mkt from select realized_vol: (log last price)%log first price by input.time xbar time.minute, date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time);
        select realized_vol: dev realized_vol by date, sym, listing_mkt from select realized_vol: (log last price)%log first price by input.time xbar time.minute, date, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)];
    };

//Trade Intensity
.mapq.summarystats.realizedvolatility:{[input.trades; input.time; input.start.time; input.end.time]
    :$[`mkt in cols input.trades; 
        select trade_intensity: %[;1e9]"f"$avg 1_deltas time  by date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time);
        select trade_intensity: %[;1e9]"f"$avg 1_deltas time  by date, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)];
    };

//Quote Intensity
.mapq.summarystats.realizedvolatility:{[input.quotes; input.time; input.start.time; input.end.time]
    :$[`mkt in cols input.trades; 
        select trade_intensity: %[;1e9]"f"$avg 1_deltas time  by date, mkt, sym, listing_mkt from input.quotes where time within(input.start.time;input.end.time);
        select trade_intensity: %[;1e9]"f"$avg 1_deltas time  by date, sym, listing_mkt from input.quotes where time within(input.start.time;input.end.time)];
    };

//Variation Ratio
.mapq.summarystats.variationratio:{[input.quotes; input.time; input.start.time; input.end.time]
    sample_1m: `sym`time xasc (`minute`date)_update time: minute+date from 0!select var_ratio_1m: var 100* (log next mid)%log mid by input.time xbar time.minute, date, sym, listing_mkt from input.quotes where time within(input.start.time;input.end.time);
    sample_5m: `sym`time xasc (`minute`date)_update time: (minute)+date from 0! select var_ratio_5m:  var 100* (log next mid)%log mid by (5*input.time) xbar time.minute, date, sym, listing_mkt from input.quotes where time within(input.start.time;input.end.time);
    select vr: (avg var_ratio_5m)%5* avg var_ratio_1m by date: `date$time, sym, listing_mkt from (wj1[sample_1m.time -/:(5*input.time-1;00:00:00); `sym`time; sample_1m;(sample_5m;(avg;`var_ratio_5m))]) where var_ratio_1m<>0
    };

//Autocorrelation Ratio
.mapq.summarystats.variationratio:{[input.quotes; input.time; input.start.time; input.end.time]
    sample_1m: `sym`time xasc (`minute`date)_update time: minute+date from 0!select var_ratio_1m: var 100* (log next mid)%log mid by input.time xbar time.minute, date, sym, listing_mkt from input.quotes where time within(input.start.time;input.end.time);
    sample_5m: `sym`time xasc (`minute`date)_update time: (minute)+date from 0! select var_ratio_5m:  var 100* (log next mid)%log mid by (5*input.time) xbar time.minute, date, sym, listing_mkt from input.quotes where time within(input.start.time;input.end.time);
    select vr: (avg var_ratio_5m)%5* avg var_ratio_1m by date: `date$time, sym, listing_mkt from (wj1[sample_1m.time -/:(`timespan$*[;1e9]"f"$(5*input.time)-1;00:00:00.000); `sym`time; sample_1m;(sample_5m;(avg;`var_ratio_5m))]) where var_ratio_1m<>0
    };


.mapq.summarystats.variationratio[getData.edwQ; 1; 09:30:00; 10:30:00]
.mapq.summarystats.realizedvolatility:{[input.trades; input.time; input.start.time; input.end.time]
    :$[`mkt in cols input.trades; 
        select var_ratio: dev realized_vol by date, mkt, sym, listing_mkt from 
        (select var_ratio_5: (log last price)%log first price by (5*input.time) xbar time.minute, date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)) lj select var_ratio: (log first price)%log last price by input.time xbar time.minute, date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time);
        select realized_vol: dev realized_vol by date, sym, listing_mkt from select realized_vol: (log first price)%log last price by input.time xbar time.minute, date, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)];
    };

numLags:6    / how many lagged terms are to be accounted for?
isTrend:1b   / is there a trend to be accounted for?

ARmdl:.ml.ts.AR.fit[yTrainSales;xTrainSales;numLags;isTrend]

params:.var.kwargs`p`trend!(3;0b)
q)show mdl2:.ml.kxi.ts.AR.fit[endog;exog;params]

.ml.ts.AR.fit[endog;exog;params]

log(cls([]sym:x))`close

First, to encapsulate the linear regression fitting, we’ll develop a function called ab. This function takes a pair of indices as input, 
retrieves data from the cls table (which we used in the cointegration test), and uses the lrf function to obtain the linear regression parameters. 
The implementation would look like this:

ab:{lrf . log(cls([]sym:x))`close}

Next, we’ll encapsulate the spread calculation. We’ll create a function that takes a pair of indices and returns another function. 
This returned function will expect the prices of x and y as inputs and calculate the spread. 
Leveraging q’s conciseness, we can implement this as follows:

sm:{[a;b;x;y]y-a+b*x}. ab@








//Trade Volatilty IIROC
.mapq.summarystats.tradevoliiroc:{[input.trades; input.start.time; input.end.time]
    :$[`mkt in cols input.trades; 
        select (max[price]-min[price])%by date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time);
        select realized_vol: dev ((next log price) - log price) by date, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)];
    };

//Time-Weigthed Average Price over Last 30 mins
.mapq.summarystats.twapcprice:{[input.table; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table;
    :$[`mkt in cols input.table;
        select twap_closing_price_q: ((next time) - time) wavg 0.5*bid+ask  by date, mkt, sym, listing_mkt from input.table where time within(input.start.time;input.end.time); 
        select twap_closing_price_q: ((next time) - time) wavg 0.5*bid+ask  by date, sym, listing_mkt from input.table where time within(input.start.time;input.end.time)]
    /eval (?;`Quotes;enlist enlist(within;`time;(enlist;15:50:00.000;15:59:50.000));`date`sym!`date`sym;(enlist`twap_closing_price)!enlist(wavg;(-;next;`time);`time);(*;0.5;(+;`nat_best_offer;`nat_best_bid)))
    };

//Intraday Variation Ratio
.mapq.summarystats.realizedvolatility:{[input.trades; input.start.time; input.end.time]
    :$[`mkt in cols input.trades; 
        select realized_vol: dev ((next log price) - log price) by date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time);
        select realized_vol: dev ((next log price) - log price) by date, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)];
    };





〖VR〗_d=(Var(R_(t,5min)))/(5Var(R_(t,1min)))
//Intraday Liquidity Metrics

.mapq.summarystats.filtertrades:{[tt]
// Trade-based filters
    tt:@[;`sym;`p#] `sym xasc 0!update date:`date$time from tt where event=`Trade, not trade_correction="Y", not trade_stat=`C, not s_internal_cross="Y";  /eval (!;0;(!;`tt;enlist enlist((/:;in);enlist`Trade;`event);`date`sym`listing_mkt`time!`date`sym`listing_mkt`time;`price`volume`total_value!((wavg;`volume;`price);(sum;`volume);(prd;(enlist;`price;`volume)))))
    : select from tt where date<>0Nd;
    }; 




.mapq.summarystats.filterorders:{[OO]
// Order-based filters
    OO: eval (!;0;(?;`getData.edwO;enlist((in;`event;enlist`Order);(>;`price;0);(>;`volume;0));0b;()));
    : @[;`instrumentID;`p#] `instrumentID xasc OO;
    }; 

.mapq.summarystats.filterquotes:{[QQ]
// Quote-based filters
    QQ: eval (!;0;(?;QQ;enlist ((>;`nat_best_bid;0);(<;`nat_best_offer;0W));0b;()));
    QQ:eval (!;QQ;();0b;(enlist`date)!enlist($;enlist`date;`time));
    :@[;`sym;`p#] `sym xasc QQ;
    }; 

//buy or sell trade
.mapq.summarystats.ticks:{[input.table]
    :$[`nat_best_bid in cols input.table;
        select tick: ?[b_active_passive=s_active_passive;?[(price>0.5*nat_best_bid+nat_best_offer) or (price=0.5*nat_best_bid+nat_best_offer and price > prev price and sym = prev sym);1;-1];?[b_active_passive="A";1;-1]] from input.table;
        select tick: ?[b_active_passive=s_active_passive;?[(price>0.5*ask+bid) or (price=0.5*ask+bid and price > prev price and sym = prev sym);1;-1];?[b_active_passive="A";1;-1]] from input.table]
    };

//join trade and quotes
.mapq.summarystats.tradesnquotes:{[input.trades;input.quotes]
    t:aj[`sym`time;input.trades;update time_quote: time from input.quotes];
    t: @[;`sym;`p#] `sym xasc ((),`time_quote)_select from t where ((time-time_quote) < 0D00:00:10.000), time_quote<>0Np;
    :t,'.mapq.summarystats.ticks t;
    };

//summary stats quotes last 5 mins
.mapq.summarystats.summarystatsquotes:{[input.table; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table;
    :$[`mkt in cols input.table; 
    select maxbid:max bid, min_ask:min ask,
    last_bid:avg bid, last_ask:avg ask, last_mid_price:(avg[bid]+avg[ask])%2
        by date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time); 
    select maxbid:max bid, min_ask:min ask,
    last_bid:avg bid, last_ask:avg ask, last_mid_price:(avg[bid]+avg[ask])%2
        by date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };

//summary stats trades
.mapq.summarystats.summarystatstrades:{[input.table; input.start.time; input.end.time]
     :$[`mkt in cols input.table; 
    select total_volume:sum[volume], total_value:sum[total_value], vwap:wavg[volume;price], adv: avg volume, range:max[price]-min[price], 
    maxprice:max price, minprice:min price, last_price: last price, num_of_trades: count i, 
    num_of_block_trades: sum raze ?[(volume>=10000) or (total_value>=200000);1;0], 
    block_volume: sum raze ?[(volume>=10000) or (total_value>=200000);volume;0], 
    block_value: sum raze ?[(volume>=10000) or (total_value>=200000);total_value;0],
        dark_volume: sum raze ?[(b_dark="Y"|s_dark="Y");volume;0], dark_value: sum raze ?[(b_dark="Y"|s_dark="Y");total_value;0],
        num_of_dark_trades: sum raze ?[(b_dark="Y"|s_dark="Y");1;0]
    by date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time);
        select total_volume:sum[volume], total_value:sum[total_value], vwap:wavg[volume;price], adv: avg volume, range:max[price]-min[price], 
    maxprice:max price, minprice:min price, last_price: last price, num_of_trades: count i, 
    num_of_block_trades: sum raze ?[(volume>=10000) or (total_value>=200000);1;0], 
    block_volume: sum raze ?[(volume>=10000) or (total_value>=200000);volume;0], 
    block_value: sum raze ?[(volume>=10000) or (total_value>=200000);total_value;0],
        dark_volume: sum raze ?[(b_dark="Y"|s_dark="Y");volume;0], dark_value: sum raze ?[(b_dark="Y"|s_dark="Y");total_value;0],
        num_of_dark_trades: sum raze ?[(b_dark="Y"|s_dark="Y");1;0]
        by date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };


//Time-Weigthed Average Price over Period of Time
.mapq.summarystats.twapcprice:{[input.table; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table;
    :$[`mkt in cols input.table;
        select twap_closing_price: ((next time) - time) wavg 0.5*bid+ask  by date, mkt, sym, listing_mkt from input.table where time within(input.start.time;input.end.time); 
        select twap_closing_price: ((next time) - time) wavg 0.5*bid+ask  by date, sym, listing_mkt from input.table where time within(input.start.time;input.end.time)]
    /eval (?;`Quotes;enlist enlist(within;`time;(enlist;15:50:00.000;15:59:50.000));`date`sym!`date`sym;(enlist`twap_closing_price)!enlist(wavg;(-;next;`time);`time);(*;0.5;(+;`nat_best_offer;`nat_best_bid)))
    };


//Bid-Ask Spreads/Quoted Spreads
.mapq.summarystats.qs:{[input.table; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table; /rename bid and ask columns to bid_price and ask_price
    :$[`mkt in cols input.table;
        select dqs: ((next time) - time) wavg (ask- bid), pqs: ((next time) - time) wavg ((ask- bid)%(0.5*ask+bid)), num_quotes: count i by date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time); 
        select dqs: ((next time) - time) wavg (ask- bid), pqs: ((next time) - time) wavg ((ask- bid)%(0.5*ask+bid)), num_quotes: count i by date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };

//Effective Spreads
.mapq.summarystats.es:{[input.table; input.columns; input.start.time; input.end.time] 
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table; /rename bid and ask columns to bid_price and ask_price
    :$[`mkt in cols input.table; 
             select des_k: volume wavg (2*tick*(price-0.5*bid+ask)), pes_k:volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time); /calculate effective spread for kth trade and aggregate using volume weighted avg
             select des_k: volume wavg (2*tick*(price-0.5*bid+ask)), pes_k:volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };

//Short Trade Statistics
.mapq.summarystats.shorttrades:{[input.table; input.start.time; input.end.time] 
    /add helper function that detects if all columns that are needed are part of input  something like if[not x in cols t;t:![t;();0b;(enlist x)!enlist 0]];
    input.table: select from input.table where (s_type<>`ST) or (s_type<>`IN), s_short="Y", (s_short_marking_exempt<>"Y") or (b_short_marking_exempt<>"Y"), s_market_maker<>"Y", b_market_maker<>"Y", s_program_trade<>"Y",not b_user_name~s_user_name;
    :$[`mkt in cols input.table; 
             select short_volume: sum raze volume, total_short_value: sum raze total_value, num_of_short_trades: count i by date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time);
             select short_volume: sum raze volume, total_short_value: sum raze total_value, num_of_short_trades: count i by date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };


//Orderbook Depth
.mapq.summarystats.midorderbook:{[input.table;input.columns;input.start.time; input.end.time] 
    /WEIGHTED MID FOR N LEVELS OF AN ORDER BOOK
    bid_price: input.columns where input.columns like "*bid_p*";nat_bid_price: input.columns where input.columns like "nat_best_bid"; ask_price: input.columns where input.columns like "*ask_p*";offer_price: input.columns where input.columns like "nat_best_offer"; /get column names for input bid and ask columns 
    bid_size: input.columns where  input.columns like "*bid_size*" ;ask_size: input.columns where input.columns like "*ask_size*"; offer_size: input.columns where input.columns like "*offer_size*";
    input.table: ((bid_price,nat_bid_price,ask_price,offer_price,bid_size,ask_size,offer_size)!`bid`ask`bid_size`ask_size)xcol input.table;
    :$[`mkt in cols input.table;
        select wmid: sum (bid_size; ask_size) wavg (bid; ask) by date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time);
        select wmid: sum (bid_size; ask_size) wavg (bid; ask) by date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };

//Realized Spreads
.mapq.summarystats.rs: {[input.trades;input.quotes;input.time; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns like "*bid_p*"; nat_bid_price: input.columns where input.columns like "nat_best_bid"; ask_price: input.columns where input.columns like "*ask_p*";offer_price: input.columns where input.columns like "nat_best_offer";
    input.quotes: ((bid_price,nat_bid_price,ask_price,offer_price)!`bid`ask)xcol input.quotes;
    ttnQQ: .mapq.summarystats.tradesnquotes[input.trades; update time: input.time +\: time from input.quotes];
    :$[`mkt in cols ttnQQ;
        select drs_k: volume wavg (2*tick*(price-0.5*bid+ask)), prs_k: volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by date, mkt, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time);
        select drs_k: volume wavg (2*tick*(price-0.5*bid+ask)), prs_k: volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by date, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time)]
   };

//Price Impact
.mapq.summarystats.pi: {[input.trades;input.quotes;input.time; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns like "*bid_p*"; nat_bid_price: input.columns where input.columns like "nat_best_bid"; ask_price: input.columns where input.columns like "*ask_p*";offer_price: input.columns where input.columns like "nat_best_offer";
    ttnQQ: .mapq.summarystats.tradesnquotes[input.trades;input.quotes];
    ttnQQ: ((bid_price,nat_bid_price,ask_price,offer_price)!`bid`ask)xcol ttnQQ;
    input.quotes_forward: update time: input.time +\: time from ((bid_price,nat_bid_price,ask_price,offer_price)!`bid_price`ask_price)xcol input.quotes;
    ttnQQ: .mapq.summarystats.tradesnquotes[ttnQQ; select sym, time, bid_price, ask_price from input.quotes_forward];
    :$[`mkt in cols ttnQQ;
        select dpi_k: volume wavg (2*tick*((0.5*bid_price+ask_price)-0.5*bid+ask)), ppi_k: volume wavg (2*tick*((0.5*bid_price+ask_price)-0.5*bid+ask))%(0.5*bid+ask) by date, mkt, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time);
        select dpi_k: volume wavg (2*tick*((0.5*bid_price+ask_price)-0.5*bid+ask)), ppi_k: volume wavg (2*tick*((0.5*bid_price+ask_price)-0.5*bid+ask))%(0.5*bid+ask) by date, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time)]
   };


//OrderBook Imbalance
.mapq.summarystats.orderbookimbalance:{[input.trades; input.start.time; input.end.time]
    input.trades: $[`mkt in cols input.trades; 
             select volumebuy: sum raze ?[b_active_passive="A"; volume;0],  volumesell:  sum raze ?[s_active_passive="A"; volume;0] by date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time);
             select volumebuy: sum raze ?[b_active_passive="A"; volume;0],  volumesell:  sum raze ?[s_active_passive="A"; volume;0] by date, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)];
    :update orderbookimbalance: (volumebuy-volumesell)%volumebuy+volumesell from  input.trades;
    };

//Realized Volatilty
.mapq.summarystats.realizedvolatility:{[input.trades; input.start.time; input.end.time]
    :$[`mkt in cols input.trades; 
        select realized_vol: sqrt avg ((next price) - price) xexp 2 by date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time);
        select realized_vol: sqrt avg ((next price) - price) xexp 2 by date, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)];
    };

//Bid Depth and Ask Depth by Broker from Order data
mm_orders:{[input.orders; input.start.time; input.end.time] 
    bids: select bid_depth: sum volume*price by po: b_po, instrumentID, listing_mkt, date: `date$eventTimestamp, ac_type: b_type, sme: b_sme from orders where b_po<>0,b_type<>`, eventTimestamp within(input.start.time;input.end.time);
    asks: select ask_depth: sum volume*price by po: s_po, instrumentID, listing_mkt, date: `date$eventTimestamp, ac_type: s_type, sme: s_sme from orders where s_po<>0,s_type<>`, eventTimestamp within(input.start.time;input.end.time);
    :0!(uj/)(bids;asks);
    };


/
OMID Functions
\


// Market Quality Calculations in KDB/q

// Get Closing Price last 30 mins intraday
getCPrice:{[input;closingPriceType]
    trades:select from input where event=`Trade;
    $[closingPriceType=`last;
        [flag:time=max time;
         exec avg price from trades where flag];
        closingPriceType=`last30min;
        [t2:max time;
         t1:t2-00:30:00.000000000;
         flag:(time>=t1)&(time<=t2);
         exec avg price from trades where flag]
    ]
 };

// Daily trade volume
getTradeVol:{[input]
    exec sum volume+undisclosed_volume from input where event=`Trade
 };

// Daily traded value
getTradeVal:{[input]
    exec sum (price*undisclosed_volume)+total_value from input where event=`Trade
 };

// Daily number of trades
getTradeNum:{[input]
    exec sum num_same_time_trades from input where event=`Trade
 };

// Average daily Volume
getTradeSize:{[input;itype]
    trades:select from input where event=`Trade;
    $[itype=`Volume;
        getTradeVol[trades]%getTradeNum[trades];
        itype=`Value;
        getTradeVal[trades]%getTradeNum[trades];
        0n
    ]
 };

// Trade Volatility
getTradeVolatility:{[input;closingPriceType]
    trades:select from input where event=`Trade;
    startTime:09:35:00.000000000+`time$trades[`time][0];
    endTime:15:55:00.000000000+`time$trades[`time][0];
    validTrades:select from trades where time within(startTime;endTime);
    $[count validTrades;
        [maxPrice:exec max price from validTrades;
         minPrice:exec min price from validTrades;
         cPrice:getCPrice[validTrades;closingPriceType];
         100*(maxPrice-minPrice)%cPrice];
        0n
    ]
 };

// Intraday Volatility (simplified version without resampling)
getIntradayVolatility:{[input]
    trades:select from input where event=`Trade;
    startTime:09:35:00.000000000+`time$trades[`time][0];
    endTime:15:55:00.000000000+`time$trades[`time][0];
    validTrades:select from trades where time within(startTime;endTime);
    $[count validTrades;
        [returns:100*log (1_price)%(-1_price);
         annFactor:sqrt 251*6.5*60;  // Assuming 1-minute intervals
         dev[returns]*annFactor];
        0n
    ]
 };

// IIROC Volatility
getIIROCVolatility:{[input]
    trades:select from input where event=`Trade;
    startTime:09:35:00.000000000+`time$trades[`time][0];
    endTime:15:55:00.000000000+`time$trades[`time][0];
    validTrades:select from trades where time within(startTime;endTime);
    $[count validTrades;
        log (max price)%(min price) from validTrades;
        0n
    ]
 };

// Trade Intensity
getTradeIntensity:{[input]
    trades:select from input where event=`Trade;
    avgTimeDiff:avg deltas trades[`time];
    `float$avgTimeDiff%1000000000  // Convert nanoseconds to seconds
 };

// Hidden Trade Rate
getHiddenTradeRate:{[input]
    hiddenTrades:exec sum num_same_time_trades from input where event=`Trade,undisclosed_volume<>0;
    100*hiddenTrades%getTradeNum[input]
 };

// Hidden Trade Volume
getHiddenTradeVol:{[input]
    hiddenVol:exec sum undisclosed_volume from input where event=`Trade;
    hiddenVol%getTradeVol[input]
 };

// Quoted Depth
getQuotedDepth:{[input]
    quotes:select from input where event<>`Trade;
    avg (nat_best_bid_size+nat_best_offer_size)%2 from quotes
 };

// Number of Quotes
getQuoteNum:{[input]
    count select from input where event<>`Trade
 };

// Quote Size (shares/quote)
getQuoteSizeVol:{[input]
    quotes:select from input where event<>`Trade;
    size:exec (sum bid_size+sum ask_size)%2 from quotes;
    size%count quotes
 };

// Quote Size ($/quote)
getQuoteSizeVal:{[input]
    quotes:select from input where event<>`Trade;
    value:exec (sum bid_size*bid_price+sum ask_size*ask_price)%2 from quotes;
    value%count quotes
 };

// Quote Intensity
getQuoteIntensity:{[input]
    quotes:select from input where event<>`Trade;
    avgTimeDiff:avg deltas quotes[`time];
    `float$avgTimeDiff%1000000000  // Convert nanoseconds to seconds
 };

// Relative Quoted Spread
getRelQuotedSpread:{[input]
 quotes:select from input where (bid_price<>0)&(ask_price<>0);
 spreadLog:100*log ask_price%bid_price from quotes;
 timeDiffs:deltas[quotes`time]%1e9; // Convert to seconds
 totalTime:sum timeDiffs;
 (sum spreadLog*timeDiffs)%totalTime
 };

// Relative Effective Spread
getRelEffectiveSpread:{[input]
 trades:select from input where (ask_price_pre<>0)&(bid_price_pre<>0)&(price<>0);
 active:?[trades;
 ((=;`b_active_passive;enlist`A)&(<>;`s_active_passive;enlist`A));
 1;
 ?[trades;
 ((=;`s_active_passive;enlist`A)&(<>;`b_active_passive;enlist`A));
 -1;
 0n
 ]
 ];
 midPrice:(trades`ask_price_pre+trades`bid_price_pre)%2;
 spread:100*active*log[trades`price%midPrice]*trades`volume;
 (sum spread)%sum trades`volume
 };

// Relative Realized Spread
getRelRealizedSpread:{[input]
 trades:select from input where (ask_price_post<>0)&(bid_price_post<>0)&(price<>0);
 active:?[trades;
 ((=;`b_active_passive;enlist`A)&(<>;`s_active_passive;enlist`A));
 1;
 ?[trades;
 ((=;`s_active_passive;enlist`A)&(<>;`b_active_passive;enlist`A));
 -1;
 0n
 ]
 ];
 midPrice:(trades`ask_price_post+trades`bid_price_post)%2;
 spread:100*active*log[trades`price%midPrice]*trades`volume;
 (sum spread)%sum trades`volume
 };

// Relative Price Impact
getRelPriceImpact:{[input]
 trades:select from input where (ask_price_post<>0)&(bid_price_post<>0)&(ask_price_pre<>0)&(bid_price_pre<>0);
 active:?[trades;
 ((=;`b_active_passive;enlist`A)&(<>;`s_active_passive;enlist`A));
 1;
 ?[trades;
 ((=;`s_active_passive;enlist`A)&(<>;`b_active_passive;enlist`A));
 -1;
 0n
 ]
 ];
 midPricePre:(trades`ask_price_pre+trades`bid_price_pre)%2;
 midPricePost:(trades`ask_price_post+trades`bid_price_post)%2;
 impact:100*active*log[midPricePre%midPricePost]*trades`volume;
 (sum impact)%sum trades`volume
 };

// National Relative Quoted Spread
getNatRelQuotedSpread:{[input]
 quotes:select from input where (nat_best_bid<>0)&(nat_best_offer<>0);
 spreadLog:100*log nat_best_offer%nat_best_bid from quotes;
 timeDiffs:deltas[quotes`time]%1e9; // Convert to seconds
 totalTime:sum timeDiffs;
 (sum spreadLog*timeDiffs)%totalTime
 };

// National Relative Effective Spread
getNatRelEffectiveSpread:{[input]
 trades:select from input where (nat_best_offer_pre<>0)&(nat_best_bid_pre<>0)&(price<>0);
 active:?[trades;
 ((=;`b_active_passive;enlist`A)&(<>;`s_active_passive;enlist`A));
 1;
 ?[trades;
 ((=;`s_active_passive;enlist`A)&(<>;`b_active_passive;enlist`A));
 -1;
 0n
 ]
 ];
 midPrice:(trades`nat_best_offer_pre+trades`nat_best_bid_pre)%2;
 spread:100*active*log[trades`price%midPrice]*trades`volume;
 (sum spread)%sum trades`volume
 };

// National Relative Realized Spread
getNatRelRealizedSpread:{[input]
 trades:select from input where (nat_best_offer_post<>0)&(nat_best_bid_post<>0)&(price<>0);
 active:?[trades;
 ((=;`b_active_passive;enlist`A)&(<>;`s_active_passive;enlist`A));
 1;
 ?[trades;
 ((=;`s_active_passive;enlist`A)&(<>;`b_active_passive;enlist`A));
 -1;
 0n
 ]
 ];
 midPrice:(trades`nat_best_offer_post+trades`nat_best_bid_post)%2;
 spread:100*active*log[trades`price%midPrice]*trades`volume;
 (sum spread)%sum trades`volume
 };

// National Relative Price Impact
getNatRelPriceImpact:{[input]
 trades:select from input where (nat_best_offer_post<>0)&(nat_best_bid_post<>0)&(nat_best_offer_pre<>0)&(nat_best_bid_pre<>0);
 active:?[trades;
 ((=;`b_active_passive;enlist`A)&(<>;`s_active_passive;enlist`A));
 1;
 ?[trades;
 ((=;`s_active_passive;enlist`A)&(<>;`b_active_passive;enlist`A));
 -1;
 0n
 ]
 ];
 midPricePre:(trades`nat_best_offer_pre+trades`nat_best_bid_pre)%2;
 midPricePost:(trades`nat_best_offer_post+trades`nat_best_bid_post)%2;
 impact:100*active*log[midPricePre%midPricePost]*trades`volume;
 (sum impact)%sum trades`volume
 };

// Amihud Illiquidity (simplified version without resampling)
getAmihudIlliquidity:{[input]
 trades:select from input where event=`Trade;
 returns:100*log (1_price)%(-1_price) from trades;
 absReturns:abs returns;
 dollarVolume:getCPrice[input;`last]*getTradeVol[input];
 (sum absReturns)%(dollarVolume*1e6)
 };


// Auto-correlation
getAutoCorr:{[input;samplingPeriod]
    midPrice:(input`bid_price+input`ask_price)%2;
    // Note: Implement a proper resampling function for accurate results
    resampledPrice:midPrice;  // Placeholder for resampled price
    returns:100*log (1_resampledPrice)%(-1_resampledPrice);
    validReturns:returns where (-1_resampledPrice)<>0;
    // Note: This is a simplified autocorrelation calculation
    // For more accurate results, implement a proper ACF function
    correlation:cor[validReturns;1_validReturns,0n];
    abs first correlation
 };

// Variance Ratio
getVarRatio:{[input;frequency]
    midPrice:(input`bid_price+input`ask_price)%2;
    $[frequency=`1sec;
        [r1s:100*log (1_resample[midPrice;input`time;`1sec])%(-1_resample[midPrice;input`time;`1sec]);
         r10s:100*log (1_resample[midPrice;input`time;`10sec])%(-1_resample[midPrice;input`time;`10sec]);
         (var r10s)%(10*var r1s)];
      frequency=`10sec;
        [r10s:100*log (1_resample[midPrice;input`time;`10sec])%(-1_resample[midPrice;input`time;`10sec]);
         r1m:100*log (1_resample[midPrice;input`time;`1min])%(-1_resample[midPrice;input`time;`1min]);
         (var r1m)%(6*var r10s)];
      frequency=`1min;
        [r1m:100*log (1_resample[midPrice;input`time;`1min])%(-1_resample[midPrice;input`time;`1min]);
         r5m:100*log (1_resample[midPrice;input`time;`5min])%(-1_resample[midPrice;input`time;`5min]);
         (var r5m)%(5*var r1m)];
      0n]
 };

// Generate Value-Weighted Market Return
genVWMR:{[input;predictors;samplingPeriod]
    midPrice:(input`bid_price+input`ask_price)%2;
    // Note: Implement a proper resampling function for accurate results
    resampledPrice:midPrice;  // Placeholder for resampled price
    100*log (1_resampledPrice)%(-1_resampledPrice)
 };

// R-squared
getRSQ:{[input;predictors;samplingPeriod]
    midPrice:(input`bid_price+input`ask_price)%2;
    // Note: Implement a proper resampling function for accurate results
    resampledPrice:midPrice;  // Placeholder for resampled price
    returns:100*log (1_resampledPrice)%(-1_resampledPrice);
    marketRet:predictors`market_return;
    predictorsMat:flip(
        10_marketRet;
        -1_-1_marketRet;
        -2_-2_marketRet;
        -3_-3_marketRet;
        -4_-4_marketRet;
        -5_-5_marketRet;
        -6_-6_marketRet;
        -7_-7_marketRet;
        -8_-8_marketRet;
        -9_-9_marketRet;
        -10_-10_marketRet;
        10_predictors`market_cap;
        10_predictors`closing_price
    );
    .ml.linreg[10_returns;predictorsMat][`rsquared]
 };

// CMAX (placeholder function)
getCMAX:{[input]
    0  // Placeholder return value
 };

// Delay
getDelay:{[input;predictors;samplingPeriod]
    midPrice:(input`bid_price+input`ask_price)%2;
    // Note: Implement a proper resampling function for accurate results
    resampledPrice:midPrice;  // Placeholder for resampled price
    returns:100*log (1_resampledPrice)%(-1_resampledPrice);
    marketRet:predictors`market_return;
    predictorsMatUnres:flip(
        10_marketRet;
        -1_-1_marketRet;
        -2_-2_marketRet;
        -3_-3_marketRet;
        -4_-4_marketRet;
        -5_-5_marketRet;
        -6_-6_marketRet;
        -7_-7_marketRet;
        -8_-8_marketRet;
        -9_-9_marketRet;
        -10_-10_marketRet;
        10_predictors`market_cap;
        10_predictors`closing_price
    );
    predictorsMatRes:flip(
        10_marketRet;
        10_predictors`market_cap;
        10_predictors`closing_price
    );
    r2Unres:.ml.linreg[10_returns;predictorsMatUnres][`rsquared];
    r2Res:.ml.linreg[10_returns;predictorsMatRes][`rsquared];
    100*(1-(r2Res%r2Unres))
 };

// Helper function for resampling (placeholder)
resample:{[data;time;interval]
    // Implement proper resampling logic here
    data  // Placeholder return, just returns original data
 };





























/
Order Data Identification
\

GetInputDates: {[input.start.date; input.end.date]
    dates: {[n] {x+2000.01.01}each n}[til (.z.d-2000.01.01)]; /get all days til yesterday
    calendar: desc raze (`mon;`tue;`wed;`thur;`fri)!(dates where ((5+til count dates) mod 7)= 0;dates where ((4+til count dates) mod 7)= 0;dates where ((3+til count dates) mod 7)= 0;dates where ((2+til count dates) mod 7)= 0;dates where ((1+til count dates) mod 7)= 0);
    calendar: calendar where calendar <= input.end.date;
    :0N 1#calendar 1 + til calendar?last calendar where calendar>=input.start.date;
    };
calendar: GetInputDates[2022.03.01;2022.04.01]; /2022.04.01


\\Constant Values order data
input.symbols :`ABTC`ABTC.U`ACAA`AGLB`AGSG`ALFA`APLY`ARB`ARKG`ARKK`ARKW`ATSX`AUGB.F`BANK`BASE`BASE.B`BBIG`BBIG.U`BDEQ`BDIC`BDIV`BDOP`BEPR`BEPR.U`BESG`BFIN`BFIN.U`BGC`BGU`BGU.U`BHAV`BILT`BITC`BITC.U`BITI`BITI.U`BKCC`BKL.C`BKL.F`BKL.U`BLCK`BLDR`BLOV`BMAX`BNC`BND`BPRF`BPRF.U`BREA`BRKY`BSKT`BTCC`BTCC.B`BTCC.J`BTCC.U`BTCQ`BTCQ.U`BTCX.B`BTCX.U`BTCY`BTCY.B`BTCY.U`BUFR`BXF`CACB`CAFR`CAGG`CAGS`CALL`CALL.B`CALL.U`CAN`CAPS`CAPS.B`CAPS.U`CARB`CARS`CARS.B`CARS.U`CASH`CBAL`CBCX`CBGR`CBH`CBIL`CBND`CBNK`CBO`CBON`CBON.U`CBUG`CCBI`CCDN`CCEI`CCLN`CCNS`CCOM`CCOR`CCOR.B`CCOR.U`CCRE`CCX`CDEF`CDIV`CDLB`CDLB.B`CDLB.U`CDNA`CDZ`CED`CEMI`CES`CES.B`CESG`CESG.B`CEW`CFLX`CFRT`CGAA`CGBI`CGDV`CGDV.B`CGHY`CGHY.U`CGIN`CGIN.U`CGL`CGL.C`CGLO`CGR`CGRA`CGRB`CGRB.U`CGRE`CGRN`CGRN.U`CGRO`CGXF`CGXF.U`CHB`CHCL.B`CHNA.B`CHPS`CHPS.U`CIBR`CIC`CIE`CIEH`CIEI`CIEM`CIEM.U`CIF`CINC`CINC.B`CINC.U`CINF`CINT`CINV`CINV.U`CJP`CLF`CLG`CLML`CLML.U`CLMT`CLU`CLU.C`CMAG`CMAG.U`CMAR`CMAR.U`CMCE`CMCX.B`CMCX.U`CMDO`CMDO.U`CMEY`CMEY.U`CMGG`CMGG.U`CMR`CMUE`CMUE.F`CMVX`CNAO`CNAO.U`CNCC`COMM`COPP`COW`CPD`CPLS`CQLC`CQLI`CQLU`CRED`CRED.U`CROP`CROP.U`CRQ`CRYP`CRYP.B`CRYP.U`CSAV`CSBA`CSBG`CSBI`CSCB`CSCE`CSCP`CSD`CSGE`CSY`CTIP`CUD`CUDV`CUDV.B`CUEH`CUEI`CUSA.B`CUSM.B`CUTL`CUTL.B`CVD`CWO`CWW`CXF`CYBR`CYBR.B`CYBR.U`CYH`DAMG`DAMG.U`DANC`DANC.U`DATA`DATA.B`DCC`DCG`DCP`DCS`DCU`DFC`DFD`DFE`DFU`DGR`DGR.B`DGRC`DISC`DIVS`DLR`DLR.U`DQD`DQI`DRCU`DRFC`DRFD`DRFE`DRFG`DRFU`DRMC`DRMD`DRME`DRMU`DSAE`DWG`DXB`DXC`DXDB`DXEM`DXET`DXF`DXG`DXIF`DXM`DXN`DXO`DXP`DXQ`DXR`DXU`DXV`DXW`DXZ`EAAI`EAAI.U`EAFT`EAFT.U`EAGB`EAGB.U`EARK`EARK.U`EARN`EAUT`EAUT.U`EAXP`EAXP.U`EBIT`EBIT.U`EBNK`EBNK.B`EBNK.U`EDGE`EDGE.U`EDGF`EGIF`EHE`EHE.B`ELV`EMV.B`ENCC`EPCA`EPCA.U`EPCH`EPCH.U`EPGC`EPGC.U`EPWR`EPZA`EPZA.U`EQE`EQE.F`EQL`EQL.F`EQL.U`ERCV`ERDO`ERDV`EREO`EREV`ERFO`ERFV`ERGO`ESG`ESG.F`ESGA`ESGB`ESGC`ESGE`ESGF`ESGG`ESGH`ESGH.F`ESGY`ESGY.F`ESPX`ESPX.B`ETAC`ETC`ETC.U`ETHH`ETHH.B`ETHH.J`ETHH.U`ETHI`ETHQ`ETHQ.U`ETHR`ETHR.U`ETHX.B`ETHX.U`ETHY`ETHY.B`ETHY.U`ETP`ETP.A`ETSX`EUR`EUR.A`FAI`FBAL`FBCN`FBE`FBGO`FBT`FBTC`FBTC.U`FBU`FCCB`FCCD`FCCL`FCCM`FCCQ`FCCV`FCGB`FCGB.U`FCGC`FCGI`FCGS`FCGS.U`FCHH`FCHY`FCID`FCIG`FCIG.U`FCII`FCIL`FCIM`FCIQ`FCIQ.U`FCIV`FCLC`FCLH`FCMH`FCMI`FCMO`FCMO.U`FCNS`FCQH`FCRH`FCRR`FCRR.U`FCSB`FCSI`FCSW`FCUD`FCUD.U`FCUH`FCUL`FCUL.U`FCUQ`FCUQ.U`FCUV`FCUV.U`FCVH`FDE`FDE.A`FDL`FDN`FDN.F`FDV`FEBB.F`FEQT`FETH`FETH.U`FGB`FGGE`FGO`FGO.U`FGRO`FGSG`FHB`FHG`FHG.F`FHH`FHH.F`FHI`FHI.B`FHI.U`FHIS`FHQ`FHQ.F`FIE`FIG`FIG.U`FINN`FINN.U`FINO`FINT`FIVE`FIVE.B`FIVE.U`FIXD`FJFB`FJFG`FLAM`FLB`FLBA`FLCD`FLCI`FLCP`FLDM`FLEM`FLGA`FLGD`FLI`FLJA`FLRM`FLSD`FLSL`FLUI`FLUR`FLUS`FLX`FLX.B`FLX.U`FMTV`FOUR`FPR`FQC`FSB`FSB.U`FSD`FSD.A`FSEM`FSF`FSL`FSL.A`FST`FST.A`FTB`FUD`FUD.A`FUT`FWCP`FXM`GBAL`GBND`GCBD`GCNS`GCSC`GDEP`GDEP.B`GDIV`GDPY`GDPY.B`GEQT`GGAC`GGEM`GGRO`GHD`GHD.F`GIGR`GIGR.B`GIQG`GIQG.B`GIQU`GIQU.B`GLC`GLCC`GLDE`GOGO`GPMD`GRNI`HAB`HAC`HAD`HADM`HAEB`HAF`HAJ`HAL`HARB`HARB.J`HARB.U`HARC`HAU`HAU.U`HAZ`HBA`HBAL`HBB`HBD`HBF`HBF.B`HBF.U`HBFE`HBG`HBG.U`HBGD`HBGD.U`HBIT`HBKD`HBKU`HBLK`HBU`HBUG`HCA`HCAL`HCB`HCBB`HCLN`HCN`HCON`HCRE`HDGE`HDIF`HDIV`HDOC`HEAL`HEB`HED`HEMB`HEMC`HERO`HERS`HERS.B`HESG`HEU`HEUR`HEW`HEWB`HFA`HFD`HFG`HFIN`HFMU`HFMU.U`HFP`HFR`HFT`HFU`HFY`HFY.U`HGC`HGD`HGGB`HGGG`HGM`HGR`HGRO`HGU`HGY`HHF`HHL`HHL.B`HHL.U`HHLE`HID`HID.B`HIG`HIG.U`HII`HISA`HISU.U`HIU`HIX`HLFE`HLIF`HLIT`HLPR`HMAX`HMJI`HMJR`HMJU`HMMJ`HMMJ.U`HMP`HMUS`HMUS.U`HND`HNU`HNY`HOD`HOG`HOU`HPF`HPF.U`HPR`HQD`HQD.U`HQU`HRA`HRAA`HRED`HRES`HREU`HRIF`HSAV`HSD`HSH`HSL`HSPN`HSPN.U`HSU`HSUV.U`HTA`HTA.B`HTA.U`HTAE`HTB`HTB.U`HTH`HUBL`HUBL.U`HUC`HUF`HUF.U`HUG`HUIB`HUL`HUL.U`HULC`HULC.U`HUM`HUM.U`HUN`HURA`HUTE`HUTL`HUTS`HUV`HUZ`HVAX`HWF`HXCN`HXD`HXDM`HXDM.U`HXE`HXEM`HXF`HXH`HXQ`HXQ.U`HXS`HXS.U`HXT`HXT.U`HXU`HXX`HYBR`HYDR`HYI`HYLD`HYLD.U`HZD`HZU`ICAE`ICPB`IDEF.B`IDIV.B`IEMB`IFRF`IGAF`IGB`IGCF`IGLB`IIAE`IICE`IICE.F`IITE`IITE.F`ILGB`ILV`ILV.F`INOC`INSR`IQD`IQD.B`ISIF`ISTE`ISTE.F`IUAE`IUAE.F`IUCE`IUCE.F`IUTE`IUTE.F`IWBE`IXTE`JAPN`JAPN.B`KILO`KILO.B`KILO.U`LDGR`LEAD`LEAD.B`LEAD.U`LIFE`LIFE.B`LIFE.U`LINK`LONG`LYCT`LYFR`MAYB.F`MBAL`MCKG`MCLC`MCON`MCSB`MCSM`MDIV`MDVD`MDVD.U`MEE`MEME.B`MESH`MEU`MFT`MGAB`MGB`MGRW`MGSB`MHCD`MHYB`MIND`MINF`MINN`MINT`MINT.B`MIVG`MJJ`MKB`MKC`MNU.U`MNY`MOM`MOM.F`MOM.U`MPCF`MPY`MREL`MTAV`MUB`MULC`MULC.B`MUMC`MUMC.B`MUS`MUSA`MUSC`MUSC.B`MWD`MWMN`MXU`NACO`NAHF`NALT`NBND`NCG`NDIV`NFAM`NGPE`NHYB`NINT`NNRG`NNRG.U`NOVB.F`NPRF`NREA`NRGI`NSAV`NSCB`NSCC`NSCE`NSGE`NSSB`NUBF`NUSA`NXF`NXF.B`NXF.U`NXTG`ONEB`ONEC`ONEQ`ORBT`ORBT.U`PAYF`PBD`PBI`PBI.B`PCF`PCON`PCOR`PDC`PDF`PDIV`PEU`PEU.B`PFAA`PFAE`PFCB`PFG`PFH.F`PFIA`PFL`PFLS`PFMN`PFMS`PFSS`PGB`PGL`PHE`PHE.B`PHR`PHW`PIB`PID`PIN`PINC`PINV`PLDI`PLV`PMIF`PMIF.U`PMM`PMNT`PPS`PR`PRA`PREF`PRP`PSA`PSB`PSU.U`PSY`PSY.U`PSYK`PTB`PUD`PUD.B`PXC`PXG`PXG.U`PXS`PXS.U`PXU.F`PYF`PYF.B`PYF.U`PZC`PZW`PZW.F`PZW.U`QAH`QBB`QBTL`QCB`QCD`QCE`QCH`QCLN`QCN`QDX`QDXB`QDXH`QEBH`QEBL`QEE`QEF`QEM`QGB`QGL`QHY`QIE`QIF`QINF`QMA`QMY`QQC`QQC.F`QQCC`QQCE`QQCE.F`QQEQ`QQEQ.F`QQJE`QQJE.F`QQJR`QQJR.F`QRET`QSB`QTIP`QUB`QUDV`QUIG`QUS`QUU`QUU.U`QXM`RATE`RBDI`RBNK`RBO`RBOT`RBOT.U`RCAN`RCD`RCDB`RCDC`RCE`RCEI`RCSB`RCUB`RDE`REEM`REIT`REM`REMD`RENG`RGPM`RGQQ`RGQR`RGRE`RID`RID.U`RIDH`RIE`RIE.U`RIEH`RIFI`RIG`RIG.U`RIGU`RIIN`RINT`RIRA`RIT`RLB`RLD`RLDR`RLE`RMBO`RNAG`RNAV`RPD`RPD.U`RPDH`RPF`RPS`RPSB`RPU`RPU.B`RPU.U`RQG`RQH`RQI`RQJ`RQK`RQL`RQN`RQO`RQP`RQQ`RQR`RTA`RTEC`RUBH`RUBY`RUBY.U`RUD`RUD.U`RUDB`RUDB.U`RUDC`RUDH`RUE`RUE.U`RUEH`RUSA`RUSB`RUSB.U`RWC`RWE`RWE.B`RWU`RWU.B`RWW`RWW.B`RWX`RWX.B`RXD`RXD.U`RXE`RXE.U`SBCM`SBCV`SBEA`SBQM`SBQV`SBT`SBT.B`SBT.U`SCAD`SCGI`SCGR`SEED`SFIX`SHC`SHE`SHZ`SID`SINT`SITB`SITC`SITE`SITI`SITU`SIXT`SKYY`SLVE`SLVE.U`SRIB`SRIC`SRII`SRIU`STPL`SUSA`SVR`SVR.C`SYLD`TBNK`TCBN`TCLB`TCLV`TCSB`TDB`TDOC`TDOC.U`TEC`TEC.U`TECE`TECE.B`TECE.U`TECH`TECH.B`TECH.U`TECI`TECX`TERM`TGED`TGED.U`TGFI`TGGR`TGRE`THE`THNK`THU`TIF`TILV`TIME`TINF`TIPS`TIPS.F`TIPS.U`TLF`TLF.U`TLV`TMCC`TMEC`TMEI`TMEU`TMUC`TOCA`TOCC`TOCM`TOWR`TPAY`TPE`TPRF`TPU`TPU.U`TQCD`TQGD`TQGM`TQSM`TRVI`TRVL`TRVL.U`TTP`TUED`TUED.U`TUEX`TUHY`TULB`TULV`TUSB`TUSB.U`TXF`TXF.B`TXF.U`UBIL.U`UDA`UDEF`UDEF.B`UDEF.U`UDIV`UDIV.B`UDIV.U`UHD`UHD.F`UHD.U`ULV.C`ULV.F`ULV.U`UMI`UMI.B`USB`USB.U`USCC`USCC.U`USMJ`UTIL`UXM`UXM.B`VA`VAB`VAH`VALT`VALT.B`VALT.U`VBAL`VBG`VBU`VCB`VCE`VCIP`VCN`VCNS`VDU`VDY`VE`VEE`VEF`VEH`VEQT`VFV`VGAB`VGG`VGH`VGRO`VGV`VI`VIDY`VIU`VLB`VLQ`VMO`VRE`VRIF`VSB`VSC`VSG`VSP`VUN`VUS`VVL`VVO`VXC`VXM`VXM.B`WOMN`WSGB`WSHR`WSRD`WSRI`WXM`XAGG`XAGG.U`XAGH`XAW`XAW.U`XBAL`XBB`XBM`XCB`XCBG`XCBU`XCBU.U`XCD`XCG`XCH`XCLN`XCLR`XCNS`XCS`XCSR`XCV`XDG`XDG.U`XDGH`XDIV`XDLR`XDNA`XDRV`XDSR`XDU`XDU.U`XDUH`XDV`XEB`XEC`XEC.U`XEF`XEF.U`XEG`XEH`XEI`XEM`XEMC`XEN`XEQT`XESG`XEU`XEXP`XFA`XFC`XFF`XFH`XFI`XFLB`XFN`XFR`XFS`XFS.U`XGB`XGD`XGGB`XGI`XGRO`XHAK`XHB`XHC`XHD`XHU`XHY`XIC`XID`XIG`XIGS`XIN`XINC`XIT`XIU`XLB`XLVE`XMA`XMC`XMC.U`XMD`XMH`XMI`XML`XMM`XMS`XMTM`XMU`XMU.U`XMV`XMW`XMY`XPF`XQB`XQLT`XQQ`XRB`XRE`XSAB`XSB`XSC`XSE`XSEA`XSEM`XSH`XSHG`XSHU`XSHU.U`XSI`XSMC`XSMH`XSP`XSQ`XST`XSTB`XSTH`XSTP`XSTP.U`XSU`XSUS`XTLH`XTLT`XTLT.U`XTR`XUH`XULR`XUS`XUS.U`XUSR`XUT`XUU`XUU.U`XVLU`XWD`XXM`XXM.B`YAMZ`YGOG`YTSL`YXM`YXM.B`ZACE`ZAG`ZAUT`ZBAL`ZBAL.T`ZBBB`ZBI`ZBK`ZCB`ZCDB`ZCH`ZCLN`ZCM`ZCN`ZCON`ZCPB`ZCS`ZCS.L`ZDB`ZDH`ZDI`ZDJ`ZDM`ZDV`ZDY`ZDY.U`ZEA`ZEAT`ZEB`ZEF`ZEM`ZEO`ZEQ`ZEQT`ZESG`ZEUS`ZFC`ZFH`ZFIN`ZFL`ZFM`ZFN`ZFS`ZFS.L`ZGB`ZGD`ZGEN`ZGI`ZGQ`ZGRN`ZGRO`ZGRO.T`ZGSB`ZHP`ZHU`ZHY`ZIC`ZIC.U`ZID`ZIN`ZINN`ZINT`ZJG`ZJK`ZJK.U`ZJN`ZJO`ZJPN`ZJPN.F`ZLB`ZLC`ZLD`ZLE`ZLH`ZLI`ZLU`ZLU.U`ZMBS`ZMI`ZMI.U`ZMID`ZMID.F`ZMID.U`ZMMK`ZMP`ZMSB`ZMT`ZMU`ZNQ`ZNQ.U`ZPAY`ZPAY.F`ZPAY.U`ZPH`ZPL`ZPR`ZPR.U`ZPS`ZPS.L`ZPW`ZPW.U`ZQB`ZQQ`ZRE`ZRR`ZSB`ZSDB`ZSML`ZSML.F`ZSML.U`ZSP`ZSP.U`ZST`ZST.L`ZSU`ZTIP`ZTIP.F`ZTIP.U`ZTL`ZTL.F`ZTL.U`ZTM`ZTM.U`ZTS`ZTS.U`ZUAG`ZUAG.F`ZUAG.U`ZUB`ZUD`ZUE`ZUH`ZUP`ZUP.U`ZUQ`ZUQ.F`ZUQ.U`ZUS.U`ZUS.V`ZUT`ZVC`ZVI`ZVU`ZWA`ZWB`ZWB.U`ZWC`ZWE`ZWEN`ZWG`ZWH`ZWH.U`ZWHC`ZWK`ZWP`ZWS`ZWT`ZWU`ZXM`ZXM.B`ZZZD;
input.startTime : 09:30:00.000;
input.endTime :  16:00:00.000;
input.columnsO: `eventTimestamp`instrumentID`event`b_po`s_po`price`volume; 
input.applyFilter : (in;`listing_mkt;enlist(`TSE));
input.columnsQ : `sym`time`nat_best_bid`nat_best_offer;
input.tableQ : `quote;


order_results: flip `instrumentID`date`po`avg_duration_b`id_b`avg_duration_s`id_s`bid_depth`b_orders`ask_depth`s_orders!(`symbol$();`date$(); `int$();`float$();`symbol$();`float$();`symbol$();`float$();`long$();`float$();`long$());

.mapq.summarystats.filterorders:{[OO]
// Order-based filters
    OO: eval (!;0;(?;OO;enlist((in;`event;enlist`Order`OrderAmend`OrderCancel);(>;`price;0);(>;`volume;0));0b;()));
    : @[;`instrumentID;`p#] `instrumentID xasc OO;
    }; 


mm_orders:{[t] 
     canceled :@[;`instrumentID;`p#] `instrumentID  xasc select instrumentID, date: `date$eventTimestamp, total_value: price*volume, s_po, b_po from t where event=`OrderCancel, 100000<=price*volume;
     bids: @[;`instrumentID;`p#] `instrumentID  xasc select instrumentID, eventTimestamp, date: `date$eventTimestamp, total_value: price*volume, b_po from t where 100000<=price*volume, b_po<>0n, event<>`OrderCancel;
     asks: @[;`instrumentID;`p#] `instrumentID  xasc select instrumentID, eventTimestamp, date: `date$eventTimestamp, total_value: price*volume, s_po from t where 100000<=price*volume, s_po<>0n, event<>`OrderCancel;
     bids: {cl:cols[x]inter cols y;x where not(cl#x)in cl#y}[bids;canceled];
     asks: {cl:cols[x]inter cols y;x where not(cl#x)in cl#y}[asks;canceled];
     bids_time: select avg_duration_b: avg duration, id_b: ?[3.25<=(sum duration + 16:00:00.000 - last `time$eventTimestamp )%3.6e+6;`A;`P] by instrumentID, date, po: b_po from 
     distinct update duration:%[;1e6]"f"${(where[x!y]sums y)-x}[eventTimestamp;differ total_value]  by instrumentID, date, b_po from bids;
     asks_time: select avg_duration_s: avg duration, id_s: ?[3.25<=(sum duration + 16:00:00.000 - last `time$eventTimestamp )%3.6e+6;`A;`P] by instrumentID, date, po: s_po from 
     distinct update duration:%[;1e6]"f"${(where[x!y]sums y)-x}[eventTimestamp;differ total_value]  by instrumentID, date, s_po from asks;
     :0! (uj/)(bids_time;asks_time;select bid_depth: sum total_value, b_orders: count i by instrumentID, date, po: b_po from bids;select ask_depth: sum total_value, s_orders: count i by instrumentID, date, po: s_po from asks)
    };







/Same po, total_value (order and OrderCancel pair or OrderOrderAmmend
//Inititate while loop
i:0;
/i<count[calendar]
while[i<count[calendar];
    input.startDate: last calendar[i];
    input.endDate: first calendar[i];
    
    //Get Order Data
    getData.edwO: `..getTicks[`symList`assetClass`dataType`startDate`endDate`startTime`endTime`idType`columns`applyFilter!(input.symbols;`equity;`order;input.startDate;input.endDate;input.startTime;input.endTime;`instrumentID;input.columnsO;input.applyFilter)]; /13.6m 
 
    //Filter Order Data
    orders: .mapq.summarystats.filterorders getData.edwO;
    
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `getData.edwO; /delete all records for tables in memory
    
    
    
    //Join Summary Stats and Append Results to empty table
    order_results,: mm_orders orders;
    
    
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `orders; /delete all records for tables in memory

    //Iterate again
    i+: 1;
    
    //Sleep 8 munites to bypass timeout
    {t:.z.p;while[.z.p<t+x]} 00:10:00;  
    
    ];

system "sleep 50"; // Wait for q to be ready 

-22!order_results
select distinct asc date from order_results
port:"30064";
server:hsym `$":localhost:",port;

system "q -p ",port;
system "sleep 2"; // Wait for q to be ready
server (set;`.z.pp; { system "sleep 45";});
server (set;`.z.ts; {[start;x] if[x > start + 01:00:00; exit 0]; }[.z.p;]);
server (system;"t 1000");
//########################################
//########################################
//########################################
//########################################
//########################################
//Add code to export as csv file 



//set will save the table in binary format, not csv. 
//Use 0: (which is used by save internally) to save the table in csv format with a different file name:
path: `$"C:/Users/MjaenCortes/Downloads/";
sd: 2017.12.08;
hsym[path,`$string[sd],".csv"] 0: csv 0: tab


tab:([]a:1 2 3;b:`a`b`c)
{[dir;tSym] save ` sv dir,(`$ raze string tSym,`.csv)}[hsym`C:/Users/MjaenCortes/Downloads;]  each tables[]
`:C:/Users/MjaenCortes/Downloads/tab.csv


chunk:{[n;f;data]
  h:hopen f;                                   / open file handle
  if[0=hcount f;h","sv string cols data];      / write headers to empty file
  {x raze"\n",/:1_","0:y}[h]'[n cut data];     / write chunks to file
  :hclose h;                                   / close file handle
 };

\ts getData.edwO: `..getTicks[`symList`assetClass`dataType`startDate`endDate`startTime`endTime`idType`columns`applyFilter!(input.symbols;`equity;`order;input.startDate;input.endDate;input.startTime;input.endTime;`instrumentID;input.columnsO;input.applyFilter)]; /13.6m 


output: select unique_sym: count distinct sym, total_volume: sum total_volume, total_value: sum total_value, adv: avg adv, num_of_trades: sum num_of_trades, num_of_block_trades: sum num_of_block_trades, block_volume: sum block_volume, block_value: sum block_value, dark_volume: sum dark_volume, dark_value: sum  dark_value,  num_of_dark_trades: sum num_of_dark_trades  by date, listing_mkt from all_day where date<>0Nd



/all_day,: summarystats_trades;
/intraday,: summarystats_trades;

`a xasc 1!distinct (uj). 0!/:lj'[(x;y);(y;x)]

hdb:`$":" , .z.x 0 
rs: {select from get}

n:100;
status:@[n?`3;-10?n;:;`SSS]

show window:-20 20+\:x`time

/wj1[window;`sym`time;x;(y;(sum;`px))]
/wj1[window;`sym`time;x;(y;(::;`px))]


wj1[Quotes`time;`sym`time;Quotes;(`time`sym`listing_mkt`event`b_po`s_po`price`volume xcol orders;(sum;`px))]
              orders


getData.edwT: `..getTicks[`assetClass`dataType`symList`startDate`endDate`startTime`endTime`columns`applyFilter!(`equity;
        input.tableT;
        input.symbols;
        input.startDate;input.endDate;
        input.startTime;input.endTime;
        input.columnsT;
        input.applyFilter)];
perm:{[N;L]$[N=1; L; raze .z.s[N-1;L]{x,/: y except x }\:L]} 
comb: {[N;L] distinct desc each perm[N;L]}
hlcv:([sym:()]high:();low:();price:();size:());
upd:{[t;x]hlcv::select max high,min low,last price,sum size by sym
  from(0!hlcv),select sym,high:price,low:price,price,size from x}

upd bigTrade
x: 
if[x~"last";
 upd:{[t;x].[t;();,;select by sym from x]}]
(10?1.0)+(5?100.0 + til 20)

n:10; bigTrade:([]time:.z.p+(0D00:00:10) * til n;sym:n?`AA`BB`CC`DD`EE;price:n?10.0;size:n?1000.0)

trade:`time xasc ([]time:("p"$2022.01.01) +1000*1000000*n?n;sym:raze 300#/:`MST`APPL`FB;side:n?`B`S;price:(n?1.0)+(300?1000.0+til 200),(300?100.0+til 20),(300?10.0+til 2));


//define schemas

vwap:([sym:`$()] rvwap:`float$())

trade:([]time:`timestamp$();sym:`symbol$();price:`float$();size:`float$();v:`float$();s:`float$();rvwap:`float$())



//sample 10 updates

tenUpdates:([]time:.z.p + (00:00:10) * til 10;sym:raze 5#enlist `AA`BB;price:(10?1.0)+((5?100.0 + til 20),5?10.0 + til 20)n;size:((5?1000.0),5?100.0)n:raze flip 5 cut til 10)


(10?1.0)+((5?100.0 + til 20),5?10.0 + til 20)n
// first 5 updates

d:5#tenUpdates

t:`trade

