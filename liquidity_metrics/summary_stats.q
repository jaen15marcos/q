
//Intraday Liquidity Metrics

.mapq.summarystats.filtertrades:{[tt]
// Trade-based filters
    tt: cols[tt]xcols 0!select by date:`date$time, sym, listing_mkt, sequence_number from tt;
    :@[;`sym;`p#] `sym xasc 0!update volume wavg price, sum volume by date, sym, listing_mkt, time from tt where event=`Trade, not trade_correction="Y", not trade_stat=`C;  /eval (!;0;(!;`tt;enlist enlist((/:;in);enlist`Trade;`event);`date`sym`listing_mkt`time!`date`sym`listing_mkt`time;`price`volume`total_value!((wavg;`volume;`price);(sum;`volume);(prd;(enlist;`price;`volume)))))
    }; 

.mapq.summarystats.filterorders:{[OO]
// Order-based filters
    OO: eval (!;0;(?;`getData.edwO;enlist((in;`event;enlist`Order);(>;`price;0);(>;`volume;0));0b;()));
    : @[;`instrumentID;`p#] `instrumentID xasc OO;
    }; 

.mapq.summarystats.filterquotes:{[QQ]
// Quote-based filters
    QQ: eval (!;0;(?;QQ;enlist ((>;`bid_price;0);(>;`ask_price;0);(>;`nat_best_bid;0);(<;`nat_best_offer;0W));0b;()));
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

//summary stats quotes
.mapq.summarystats.summarystatsquotes:{[input.table; input.columns; input.start.time; input.end.time]
    bid_price: input.columns where input.columns  like "*bid*";ask_price: input.columns where input.columns like "*ask*";offer_price: input.columns where input.columns like "*offer*"; /get column names for input bid and ask columns 
    input.table: ((bid_price,ask_price,offer_price)!`bid`ask)xcol input.table;
    :$[`mkt in cols input.table; 
    select maxbid:max bid, min_ask:min ask,
    last_bid:last bid, last_ask:last ask, last_mid_price:(last[bid]+last[ask])%2
        by date, mkt, sym, listing_mkt from input.table where time within (input.start.time;input.end.time); 
    select maxbid:max bid, min_ask:min ask,
    last_bid:last bid, last_ask:last ask, last_mid_price:(last[bid]+last[ask])%2
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
