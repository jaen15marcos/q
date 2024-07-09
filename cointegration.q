//from Kx - General Pivot Table
piv:{[t;k;p;v;f;g]
    / pivot t, keyed by k, on p, exposing v
    / f, a function of v and pivot values, names the columns
    / g, a function of k, pivot values, and the return of f, orders the columns
    / either can be defaulted with (::)
    / conceptually, this is
    / exec f\[v;P\]!raze((flip(p0;p1;.))!/:(v0;v1;..))\[;P\]by k0,k1,.. from t
    / where P~exec distinct flip(p0;p1;..)from t
    / followed by reordering the columns and rekeying
    v:(),v;
    G:group flip k!(t:.Q.v t)k;
    F:group flip p!t p;
    count[k]!g[k;P;C]xcols 0!key[G]!flip(C:f[v]P:flip value flip key F)!raze
    {[i;j;k;x;y]
    a:count[x]#x 0N;
    a[y]:x y;
    b:count[x]#0b;
    b[y]:1b;
    c:a i;
    c[k]:first'[a[j]@'where'[b j]];
    c}[I[;0];I J;J:where 1<>count'[I:value G]]/:\:[t v;value F]};

//Pivoting Table to pivot wider prices by date and 1 second time
prices: piv[`date`time xasc 0!select last price by sym, 1 xbar `second$time, date from Trades;(), `date`time;(), `sym; `price;{y[;0]};{x,z}]; /pivot wider table

//filling null values with preceding non-nulls
prices: fills prices; 

//get closing_price data by date
closing_prices: delete date,time from 0!select from prices where time=(last;time) fby date; 

//correlation matrix between tickers
show corr_matrix: x cor/:\:x:flip closing_prices; 

//pairwise correlation between tickers
pairwise_corr: flip (`pair1`pair2`correlation)!(raze {(count[value corr_matrix])#x} each (cols value corr_matrix);(`int$(count[value corr_matrix] xexp 2))#cols value corr_matrix;raze {raze x} each (value corr_matrix)); /pairwise correlations
pairwise_corr: select from  pairwise_corr where  pair1<>pair2, 0.9 < abs correlation;



//Stationarity test based on residulas of OLS Regression (Augmented Dickey-Fuller)

adf_test:{[t;pairs]
    pair1: first pairs;
    pair2: last pairs;
    X: ?[t[pair1] > t[pair2];t[pair2];t[pair1]]; 
    y: ?[X=t[pair1];t[pair2];t[pair1]];
    mdl: .ml.stats.OLS.fit[log X; log y; 0b];
    spread: (log y) - (first mdl[`modelInfo]`coef)*log X;
    /Augmented Dickey Fuller test on residuals
    :(value .ml.ts.stationarity spread)[`stationary]=1b};
\ts adf: adf_test[closing_prices;] peach flip pairwise_corr[`pair1`pair2];


stationary_pairs: distinct {asc x} each (flip pairwise_corr[`pair1`pair2]) where raze adf; /445
