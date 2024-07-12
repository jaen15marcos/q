GetInputDates: {[input.start.date; input.end.date]
    dates: {[n] {x+2000.01.01}each n}[til (.z.d-2000.01.01)]; /get all days til yesterday
    calendar: desc raze (`mon;`tue;`wed;`thur;`fri)!(dates where ((5+til count dates) mod 7)= 0;dates where ((4+til count dates) mod 7)= 0;dates where ((3+til count dates) mod 7)= 0;dates where ((2+til count dates) mod 7)= 0;dates where ((1+til count dates) mod 7)= 0);
    calendar: calendar where calendar <= input.end.date;
    :0N 3#calendar 1 + til calendar?last calendar where calendar>=input.start.date;
    }
calendar: GetInputDates[2024.05.01;2024.05.31];


//Constant Values
input.symbols :`;
input.startTime : 09:30:00.000;
input.endTime : 16:00:00.000;
input.columnsT : `sym`time`volume`price`total_value`listing_mkt`event`sequence_number`mkt`s_short`s_short_marking_exempt`b_short_marking_exempt`s_user_name`b_user_name`b_po_name`s_po_name`s_program_trade`b_type`s_type`b_market_maker`s_market_maker`b_active_passive`s_active_passive`trade_stat`trade_correction`s_dark`b_dark;
input.columnsQ : `sym`listing_mkt`mkt`time`nat_best_bid`nat_best_offer`nat_best_bid_size`nat_best_offer_size`ask_price`bid_price;
input.tableQ : `quote;
input.tableT : `trade;
input.applyFilter : `; 


//Create empty table to store results
output.cols: `date`mkt`sym`listing_mkt`maxbid`min_ask`last_bid`last_ask`last_mid_price`total_volume`total_value`vwap`adv`range`maxprice`minprice`last_price`num_of_trades`num_of_block_trades`block_volume`block_value`dark_volume`dark_value`num_of_dark_trades`short_volume`total_short_value`num_of_short_trades`twap_closing_price`dqs`pqs`num_quotes`des_k`pes_k`wmid`volumebuy`volumesell`orderbookimbalance`realized_vol`drs_k_1m`prs_k_1m`drs_k_5m`prs_k_5m`dpi_k_1m`ppi_k_1m`dpi_k_5m`ppi_k_5m;
dailyliqmet: flip (output.cols)!(`date$();`symbol$();`symbol$();`symbol$();`float$();`float$();`float$();`float$();`float$();`long$();``float$();`float$();`float$();`float$();`float$();`float$();`float$();`long$();`long$();`long$();`float$();`long$();`float$();`long$();`long$();`float$();`long$();`float$();`float$();`float$();`long$();`float$();`float$();`float$();`long$();`long$();`float$();`float$();`float$();`float$();`float$();`float$();`float$();`float$();`float$();`float$());
//Inititate while loop
i:0;
while[i<count[calendar];
    //Get Pairs in right order for Kalman Filter
    input.startDate: last calendar[i];
    input.endDate: first calendar[i];
    
    //Get Trade Data
    getData.edwT: `..getTicks[`assetClass`dataType`symList`startDate`endDate`startTime`endTime`columns`applyFilter!(`equity;
        input.tableT;
        input.symbols;
        input.startDate;input.endDate;
        input.startTime;input.endTime;
        input.columnsT;
        input.applyFilter)];
    
    //Filter Trade Data
    Trades: .mapq.summarystats.filtertrades getData.edwT;
    
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `getData.edwT; /delete all records for tables in memory
    
    //Get Quote Data
    getData.edwQ: `..getTicks[`assetClass`dataType`symList`startDate`endDate`startTime`endTime`columns`applyFilter!(`equity;
        input.tableQ;
        input.symbols;
        input.startDate;input.endDate;
        input.startTime;input.endTime;
        input.columnsQ;
        input.applyFilter)];
    
    //Filter Quote Data
    Quotes: .mapq.summarystats.filterquotes getData.edwQ;
    
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `getData.edwQ; /delete all records for tables in memory

    //Execute functions
    
    
    summarystatsquotes_natbidask: .mapq.summarystats.summarystatsquotes[Quotes; `nat_best_bid`nat_best_offer; input.startTime;input.endTime]; //summary stats quotes
    
    summarystats_trades: .mapq.summarystats.summarystatstrades[Trades; input.startTime;input.endTime]; //summary stats trades
    
    twap_natbidask: .mapq.summarystats.twapcprice[Quotes;`nat_best_bid`nat_best_offer;15:50:00.000; input.endTime]; //Time-Weigthed Average Price over Period of Time
    
    
    qs_natbidask: .mapq.summarystats.qs[Quotes;`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //Quoted Spreads
    
    es: .mapq.summarystats.es[.mapq.summarystats.tradesnquotes[Trades;Quotes];`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //Effective Spreads
    
    short_trades: .mapq.summarystats.shorttrades[Trades;input.startTime;input.endTime]; //Short Trade Statistics
    
    orderbook_depth: .mapq.summarystats.midorderbook[Quotes;`nat_best_bid`nat_best_offer`nat_best_bid_size`nat_best_offer_size;input.startTime;input.endTime]; //orderbook depth
    
    rs_1m: `date`mkt`sym`listing_mkt`drs_k_1m`prs_k_1m xcol .mapq.summarystats.rs[Trades;Quotes;00:01:00.000;`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //realized spreads
    rs_5m: `date`mkt`sym`listing_mkt`drs_k_5m`prs_k_5m xcol .mapq.summarystats.rs[Trades;Quotes;00:05:00.000;`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //realized spreads

    pi_1m: `date`mkt`sym`listing_mkt`dpi_k_1m`ppi_k_1m xcol .mapq.summarystats.pi[Trades;Quotes;00:01:00.000;`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //price impact
    pi_5m: `date`mkt`sym`listing_mkt`dpi_k_5m`ppi_k_5m xcol .mapq.summarystats.pi[Trades;Quotes;00:01:00.000;`nat_best_bid`nat_best_offer;input.startTime;input.endTime]; //price impact
    
    orderbook_imbalance: .mapq.summarystats.orderbookimbalance[Trades; input.startTime; input.endTime]; //orderbook_imbalance
    
    r_volatitlity: .mapq.summarystats.realizedvolatility[Trades;input.startTime;input.endTime]; //realized volatility
    
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `Trades`Quotes; /delete all records for tables in memory
    
    //Join Liquidity metrics and Append Results to empty table
    dailyliqmet,: 0!(uj/)(summarystatsquotes_natbidask;summarystats_trades;short_trades;twap_natbidask;qs_natbidask;es;orderbook_depth;orderbook_imbalance;r_volatitlity;rs_1m;rs_5m;pi_1m;pi_5m);
    
    //Sleep 5 minutes to bypass timeout
    {t:.z.p;while[.z.p<t+x]} 00:05:00;  

    //Iterate again
    i+: 1;
    ];


\\Constant Values Order data
input.symbols :`;
input.startTime : 09:30:00.000;
input.endTime :  16:00:00.000; 
input.columnsO: `eventTimestamp`instrumentID`listing_mkt`event`b_po`b_type`b_sme`s_po`s_type`s_sme`price`volume; 
input.applyFilter : ();

order_results: flip `po`instrumentID`listing_mkt`date`ac_type`sme`bid_depth`ask_depth !(`int$();`symbol$();`symbol$();`date$();`symbol$();`char$();`float$();`float$());

//Inititate while loop to calculate Ask_Depth and Bid_Depth by Broker
i:0;
while[i<count[calendar];
    input.startDate: last calendar[i];
    input.endDate: first calendar[i];
    
    //Get Order Data
    getData.edwO: `..getTicks[`symList`assetClass`dataType`startDate`endDate`startTime`endTime`idType`columns`applyFilter!(input.symbols;`equity;`order;input.startDate;input.endDate;input.startTime;input.endTime;`instrumentID;input.columnsO;input.applyFilter)]; /13.6m 

    //Filter Order Data
    orders: .mapq.summarystats.filterorders getData.edwO;
    
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `getData.edwO; /delete all records for tables in memory

    
    //Join Summary Stats and Append Results to empty table
    order_results,: mm_orders[orders;input.startTime; input.endTime];
    
    
    //Sleep 5 minutes to bypass timeout
    {t:.z.p;while[.z.p<t+x]} 00:05:00;  
    
    {[t] ![t;enlist(>;`i;-1);0b;`$()]} each `orders; /delete all records for tables in memory

    //Iterate again
    i+: 1;
    ];
