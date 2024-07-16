//Kalman Filter

kalmanFilter:{[x;y]
        // Initialize parameters
    i:0;
    while[i<count[y];
            if[i=0; 
                [
                delta:0.0001;
                Vw:(delta%1-delta)*2 2#1 0 0 1;  // 2x2 diagonal matrix
                Ve:0.001; 
                R:2 2#0f;  // initiliaze state covariance 
                P:2 2#0f;  // 2x2 zero matrix
                Kx: (2;count y)#0f; //capture Kx gain
                beta:(2;count y)#0f;  // Initialize beta with zeros
                y_est:();  // Initialize measurement prediction
                e:();  // Initialize measurement prediction error 
                Q:();  // Initialize measuremnt prediction error variance
                ]
            ];
       
            if[i>0;
                [
                beta[;i]:beta[;i-1];
                R:P+Vw;
                ]
            ];
        
            // Measurement prediction
            y_est,: sum x[i;]*beta[;i]; 
    
            //Measurement Variance
            Q,: {[M;v]:sum v*t1:M mmu v;}[R;x[i;]] + Ve;
       
            // Calculate error
            e,:y[i]-y_est[i];


            // Kalman gain
            K:mmu[R;{x*/:y}[x[i;];(1%Q[i])]];
            Kx[;i]:K; / list of Kalman gain
    

            // State update
            beta[;i]+: K*e[i];
            P:R-(x[i;] mmu R) */: Kx[;i];
            i+: 1;
        ];

        // Return updated values as a dictionary
        : flip `beta`intercept`y_est`e`Q!(beta[0];beta[1];y_est;e;Q);
    };

ResultskalmanFilter:{[t;pairs]
    //Create empty table to store results
    results: ([] beta: `float$(); intercept: `float$();  y_est:`float$(); e: `float$(); Q: `float$());
    
   //Inititate while loop
    i:0;
    while[i<count[pairs];
        //Get Pairs in right order for Kalman Filter
        pair1: first pairs[i];
        pair2: last pairs[i];
        x: (?[t[pair1] > t[pair2];t[pair2];t[pair1]]), ' 1f; 
        y: ?[x[;0]=t[pair1];t[pair2];t[pair1]];
       
        //Run Kalman Filter for i Pair
        results,: kalmanFilter[x;y];
        
        //Iterate again
        i+: 1;
        
        if[0= i mod 40; 
                [
                //Sleep 5 munites to bypass timeout
                {t:.z.p;while[.z.p<t+x]} 00:05:00;  
                ]
            ];
        
        ];
    
    
    //Add column indentifying cointegrated pair to resuts df
    : results /,' ([]tickers:raze {("i"$count[results]%count[pairs])#x} each {`$"&" sv string raze x} each pairs);
    };


\ts 
results: ResultskalmanFilter[0!prices; 100#stationary_pairs];

results: results,'([]time:raze flip {100#x} each exec time from 0!prices;date:raze flip {100#x} each exec date from 0!prices; 
    pair1: raze {(0!prices)x} each raze {first `$"&" vs string x} each {`$"&" sv string raze x} each 100#stationary_pairs; 
    pair2: raze {(0!prices)x} each raze {last `$"&" vs string x} each {`$"&" sv string raze x} each 100#stationary_pairs;
    tickers:raze {5212425#x} each {`$"&" sv string raze x} each 100#stationary_pairs);; //edit based on number of observations per ticker - check cointegration.q for more documentation
