//Intraday stats quotes by Timeframe
.mapq.intradaysummarystats.summarystatsquotes:{[input.table; input.columns; input.time.window; input.start.time; input.end.time]
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table;
    :$[`mkt in cols input.table; 
    select maxbid:max bid, min_ask:min ask,
    last_bid:last bid, last_ask:last ask, last_mid_price:(last[bid]+last[ask])%2
        by input.time.window xbar time.minute, date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time); 
    select maxbid:max bid, min_ask:min ask,
    last_bid:last bid, last_ask:last ask, last_mid_price:(last[bid]+last[ask])%2
        by input.time.window xbar time.minute, date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };

//Intraday summary stats trades by Timeframe
.mapq.intradaysummarystats.summarystatstrades:{[input.table; input.time.window; input.start.time; input.end.time]
     :$[`mkt in cols input.table; 
    select total_volume:sum[volume], total_value:sum[total_value], vwap:wavg[volume;price], adv: avg volume, range:max[price]-min[price], 
    maxprice:max price, minprice:min price, num_of_trades: count i, 
    num_of_block_trades: sum raze ?[(volume>=10000) or (total_value>=200000);1;0], 
    block_volume: sum raze ?[(volume>=10000) or (total_value>=200000);volume;0], 
    block_value: sum raze ?[(volume>=10000) or (total_value>=200000);total_value;0],
        dark_volume: sum raze ?[(b_dark="Y"|s_dark="Y");volume;0], dark_value: sum raze ?[(b_dark="Y"|s_dark="Y");total_value;0],
        num_of_dark_trades: sum raze ?[(b_dark="Y"|s_dark="Y");1;0]
    by input.time.window xbar time.minute, date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time);
        select total_volume:sum[volume], total_value:sum[total_value], vwap:wavg[volume;price], adv: avg volume, range:max[price]-min[price], 
    maxprice:max price, minprice:min price, num_of_trades: count i, 
    num_of_block_trades: sum raze ?[(volume>=10000) or (total_value>=200000);1;0], 
    block_volume: sum raze ?[(volume>=10000) or (total_value>=200000);volume;0], 
    block_value: sum raze ?[(volume>=10000) or (total_value>=200000);total_value;0],
        dark_volume: sum raze ?[(b_dark="Y"|s_dark="Y");volume;0], dark_value: sum raze ?[(b_dark="Y"|s_dark="Y");total_value;0],
        num_of_dark_trades: sum raze ?[(b_dark="Y"|s_dark="Y");1;0]
        by input.time.window xbar time.minute, date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };
 
//Intraday Time-Weigthed Average Price by Timeframe
.mapq.intradaysummarystats.twapcprice:{[input.table; input.columns; input.time.window;input.start.time; input.end.time]
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table;
    :select twap_closing_price: ((next time) - time) wavg 0.5*bid+ask  by input.time.window xbar time.minute, date, sym, listing_mkt from input.table where time within(input.start.time;input.end.time) /15:50:00.000; 15:59:50.000
    /eval (?;`Quotes;enlist enlist(within;`time;(enlist;15:50:00.000;15:59:50.000));`date`sym!`date`sym;(enlist`twap_closing_price)!enlist(wavg;(-;next;`time);`time);(*;0.5;(+;`nat_best_offer;`nat_best_bid)))
    };

//Intraday Quoted Spreads by Timeframe
.mapq.intradaysummarystats.qs:{[input.table; input.columns; input.time.window; input.start.time; input.end.time]
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table; /rename bid and ask columns to bid_price and ask_price
    :$[`mkt in cols input.table;
        select dqs: ((next time) - time) wavg (ask- bid), pqs: ((next time) - time) wavg ((ask- bid)%(0.5*ask+bid)), num_quotes: count i by input.time.window xbar time.minute, date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time); 
        select dqs: ((next time) - time) wavg (ask- bid), pqs: ((next time) - time) wavg ((ask- bid)%(0.5*ask+bid)), num_quotes: count i by input.time.window xbar time.minute, date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };

//Intraday Effective Spreads by TimeFrame
.mapq.intradaysummarystats.es:{[input.table; input.columns; input.time.window; input.start.time; input.end.time] 
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table; /rename bid and ask columns to bid_price and ask_price
    :$[`mkt in cols input.table; 
             select des_k: volume wavg (2*tick*(price-0.5*bid+ask)), pes_k:volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by input.time.window xbar time.minute, date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time); /calculate effective spread for kth trade and aggregate using volume weighted avg
             select des_k: volume wavg (2*tick*(price-0.5*bid+ask)), pes_k:volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by input.time.window xbar time.minute, date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };

//Intraday Short Trade Statistics by TimeFrame
.mapq.intradaysummarystats.shorttrades:{[input.table; input.time.window; input.start.time; input.end.time] 
    input.table: select from input.table where (s_type<>`ST) or (s_type<>`IN), s_short="Y", (s_short_marking_exempt<>"Y") or (b_short_marking_exempt<>"Y"), s_market_maker<>"Y", b_market_maker<>"Y", s_program_trade<>"Y",not b_user_name~s_user_name;
    :$[`mkt in cols input.table; 
             select short_volume: raze sum volume, total_short_value: raze sum total_value, num_of_short_trades: count i by input.time.window xbar time.minute, date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time);
             select short_volume: raze sum volume, total_short_value: raze sum total_value, num_of_short_trades: count i by input.time.window xbar time.minute, date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)]
    };


//Intraday Orderbook Depth by TimeFrame
.mapq.intradaysummarystats.midorderbook:{[input.table; input.columns; input.time.window;input.start.time; input.end.time] 
    bid_price: input.columns where input.columns like "*bid_p*";nat_bid_price: input.columns where input.columns like "nat_best_bid"; ask_price: input.columns where input.columns like "*ask_p*";offer_price: input.columns where input.columns like "nat_best_offer"; /get column names for input bid and ask columns 
    bid_size: input.columns where  input.columns like "*bid_size*" ;ask_size: input.columns where input.columns like "*ask_size*"; offer_size: input.columns where input.columns like "*offer_size*";
    input.table: ((bid_price,nat_bid_price,ask_price,offer_price,bid_size,ask_size,offer_size)!`bid`ask`bid_size`ask_size)xcol input.table;
    :$[`mkt in cols input.table; 
             select wmid: sum (ask_size;bid_size) wavg (ask;bid) by input.time.window xbar time.minute, date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time);
             select wmid: sum (ask_size;bid_size) wavg (ask;bid) by input.time.window xbar time.minute, date, sym, listing_mkt from input.table where time within (input.start.time;input.end.time)];
    };

//Intraday Realized Spreads by Timeframe
.mapq.intradaysummarystats.rs: {[input.trades;input.quotes;input.time; input.columns; input.time.window; input.start.time; input.end.time]
    bid_price: input.columns where input.columns like "*bid_p*"; nat_bid_price: input.columns where input.columns like "nat_best_bid"; ask_price: input.columns where input.columns like "*ask_p*";offer_price: input.columns where input.columns like "nat_best_offer";
    input.quotes: ((bid_price,nat_bid_price,ask_price,offer_price)!`bid`ask)xcol input.quotes;
    ttnQQ: .mapq.summarystats.tradesnquotes[input.trades; update time: input.time +\: time from input.quotes];
    :select drs_k: volume wavg (2*tick*(price-0.5*bid+ask)), prs_k: volume wavg (2*tick*(price-0.5*bid+ask))%(0.5*bid+ask) by input.time.window xbar time.minute, date, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time);
   };

//Intraday Price Impact by TimeFrame
.mapq.intradaysummarystats.pi: {[input.trades;input.quotes;input.time; input.columns; input.time.window; input.start.time; input.end.time]
    bid_price: input.columns where input.columns like "*bid_p*"; nat_bid_price: input.columns where input.columns like "nat_best_bid"; ask_price: input.columns where input.columns like "*ask_p*";offer_price: input.columns where input.columns like "nat_best_offer";
    ttnQQ: .mapq.summarystats.tradesnquotes[input.trades;input.quotes];
    ttnQQ: ((bid_price,nat_bid_price,ask_price,offer_price)!`bid`ask)xcol ttnQQ;
    input.quotes_forward: update time: input.time +\: time from ((bid_price,nat_bid_price,ask_price,offer_price)!`bid_price`ask_price)xcol input.quotes;
    ttnQQ: .mapq.summarystats.tradesnquotes[ttnQQ; select sym, time, bid_price, ask_price from input.quotes_forward];
    :select dpi_k: volume wavg (2*tick*((0.5*bid_price+ask_price)-0.5*bid+ask)), ppi_k: volume wavg (2*tick*((0.5*bid_price+ask_price)-0.5*bid+ask))%(0.5*bid+ask) by input.time.window xbar time.minute, date, sym, listing_mkt from ttnQQ where time within(input.start.time;input.end.time);
    };


//Intraday OrderBook Imbalance by Timeframe
.mapq.intradaysummarystats.orderbookimbalance:{[input.trades; input.time.window; input.start.time; input.end.time]
    input.trades: $[`mkt in cols input.trades; 
             select volumebuy: raze sum ?[b_active_passive="A"; volume;0],  volumesell:  raze sum ?[s_active_passive="A"; volume;0] by input.time.window xbar time.minute, date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time);
             select volumebuy: raze sum ?[b_active_passive="A"; volume;0],  volumesell:  raze sum ?[s_active_passive="A"; volume;0] by input.time.window xbar time.minute, date, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)];
    :update orderbookimbalance: (volumebuy-volumesell)%volumebuy+volumesell from input.trades;
    };

//Intraday Realized Volatilty by TimeFrame
.mapq.intradaysummarystats.realizedvolatility:{[input.trades; input.time.window; input.start.time; input.end.time]
    :$[`mkt in cols input.trades; 
        select realized_vol: sqrt avg ((next price) - price) xexp 2 by input.time.window xbar time.minute, date, mkt, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time);
        select realized_vol: sqrt avg ((next price) - price) xexp 2 by input.time.window xbar time.minute, date, sym, listing_mkt from input.trades where time within(input.start.time;input.end.time)];
    };
