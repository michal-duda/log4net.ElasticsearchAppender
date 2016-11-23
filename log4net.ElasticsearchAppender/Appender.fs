namespace log4net.ElasticsearchAppender

//module Implementation =

    open log4net
    open log4net.Appender
    open log4net.Core
    open System
    open System.Net
    open Types
    open System.Threading.Tasks
    open System.Collections.Generic

    module private InternalImpl = 
        open Polly
        open System.Net.Http

        let parseConnectionString (connectionString : string) bufferSize =
            let splitKeyValue (s : string) =
                let split = s.Split('=')
                (split.[0], split.[1])

            let indexName (dictionary : IDictionary<string, string>) =
                let isRolling = dictionary.ContainsKey("Rolling") && bool.Parse(dictionary.["Rolling"])
                if isRolling then 
                    dictionary.["Index"] + "_" + (DateTime.Now.Date.ToString("yyyy_MM_dd"))
                else
                    dictionary.["Index"]

            let dictionary = connectionString.Split(';') |> Array.map (fun s -> splitKeyValue s) |> dict
            {
                Server="http://" + dictionary.["Server"];
                Port=Int32.Parse(dictionary.["Port"]);
                Bulk = bufferSize > 0;
                Index=indexName dictionary
            }
          
// *************** Using polly now  
//        type Result =
//            | Success
//            | Failure of string
//        
//        type CircuitBreakerState private (maxFailCount : int, resetTimeSpan: TimeSpan, failCount: int, openTime: DateTime) = 
//                
//                let isClosed = failCount < maxFailCount
//                let isReadyForReset = (DateTime.Now - openTime) > resetTimeSpan
//
//                let prepareReturnValue res = 
//                    match res with 
//                    | Choice1Of2 _ -> 
//                        if isReadyForReset then (Success, CircuitBreakerState(maxFailCount, resetTimeSpan)) else (Success, CircuitBreakerState(maxFailCount, resetTimeSpan, failCount, openTime))
//                    | Choice2Of2 ex -> 
//                        let newFailCount = failCount + 1
//                        if newFailCount > maxFailCount then 
//                            (Failure(ex.ToString()), CircuitBreakerState(maxFailCount, resetTimeSpan, newFailCount, DateTime.Now)) 
//                        else
//                            (Failure(ex.ToString()), CircuitBreakerState(maxFailCount, resetTimeSpan, newFailCount, openTime)) 
//
//                new(maxFailCount : int, resetTimeSpan: TimeSpan) = CircuitBreakerState(maxFailCount, resetTimeSpan, 0, DateTime.MinValue) 
//
//                member x.Execute func = async {
//                    if isClosed || isReadyForReset then
//                        let! res = func() |> Async.Catch
//                        return prepareReturnValue res
//                    else
//                        return (Failure("Circuit Breaker Open"), CircuitBreakerState(maxFailCount, resetTimeSpan, failCount, openTime)) 
//                }
                
        let sendAgent (sendFunction : PostFunction<'a>) (connectionInfoProvider : unit -> ConnectionInfo) (internalErrorHandling : string -> unit) = 
            MailboxProcessor.Start(fun inbox-> 
                
                let resetTimeSpan = TimeSpan.FromMinutes(1.0)
                let maxFailureCount = 3
                // the message processing function
                let rec messageLoop = async { // (cirtuitBreaker : CircuitBreakerState) = async {
        
                    // read a message
                    let! msg = inbox.Receive()
                    let! res = sendFunction (connectionInfoProvider()) msg |> Async.Catch
                    // process a message
//                    let! res = cirtuitBreaker.Execute( fun() -> sendFunction (connectionInfoProvider()) msg )
                
                    match res with 
                        | Choice1Of2 _ -> ()
                        | Choice2Of2(ex) -> 
                            internalErrorHandling(String.Format("Elasticsearch Appender fail: {0}", ex))

                    return! messageLoop
                }
                // start the loop 
                messageLoop // (CircuitBreakerState(maxFailureCount, resetTimeSpan))
            )
        
        let policy internalErrorHanling = Policy.Handle<Exception>()
                                            .CircuitBreakerAsync(2, TimeSpan.FromMinutes(1.0), (fun ex span -> 
                                                Console.WriteLine("Failures during sending logs to Elasticsearch. Opening circuit breaker. {0}")), 
                                                (fun () -> internalErrorHanling("Cirtuit breaker reset.")))
        
        let sendFuncWithCircuitBreaker sendFunction internalErrorHanling = 
            let p = policy internalErrorHanling
            fun uri events -> async { return! p.ExecuteAsync(fun() -> sendFunction uri events |> Async.StartAsTask) |> Async.AwaitTask }

    type Appender(sendFunction : PostFunction<'a>) = 
        inherit BufferingAppenderSkeleton()
        
        let internalLogging = base.ErrorHandler.Error
        let mutable connectionInfo = {Server="";Index="";Port=0;Bulk=false}

        let agent = InternalImpl.sendAgent (InternalImpl.sendFuncWithCircuitBreaker sendFunction internalLogging) (fun () -> connectionInfo) (fun msg -> internalLogging(msg))

        new() = Appender(Communication.sendFunction())

        member val ConnectionString = "" with get, set
        
        override this.ActivateOptions() = 
            
            base.ActivateOptions()
            ServicePointManager.Expect100Continue <- false
            if String.IsNullOrEmpty(this.ConnectionString) then
                base.ErrorHandler.Error("ConnectionString is null or empty");
            else
                connectionInfo <- InternalImpl.parseConnectionString this.ConnectionString this.BufferSize

        override this.SendBuffer (events : LoggingEvent[]) = agent.Post events
            