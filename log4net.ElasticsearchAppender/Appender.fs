namespace log4net.ElasticsearchAppender

    open log4net
    open log4net.Appender
    open log4net.Core
    open System
    open System.Net
    open Types
    open System.Threading.Tasks
    open System.Collections.Generic

    module Helpers = 
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

            { Server="http://" + dictionary.["Server"]; Port=Int32.Parse(dictionary.["Port"]); Index=indexName dictionary }
            
        let policy internalErrorHanling = Policy.Handle<Exception>()
                                            .CircuitBreakerAsync(3, TimeSpan.FromMinutes(1.0), 
                                                (fun ex span -> internalErrorHanling(sprintf "Failures during sending logs to Elasticsearch. Opening circuit breaker. %O" ex)), 
                                                (fun () -> internalErrorHanling("Cirtuit breaker reset.")))
        
        let sendFuncWithCircuitBreaker sendFunction (connectionInfoProvider : unit -> ConnectionInfo) internalErrorHandling = 
            let p = policy internalErrorHandling
            fun events -> async { return! p.ExecuteAsync(fun() -> sendFunction (connectionInfoProvider()) events |> Async.StartAsTask) |> Async.AwaitTask }
    

    module ProcessingAgents =       
          
        let mailboxProcessor (sendFunction : LoggingEvent[] -> Async<'a>) (internalErrorHandling : string -> unit) = 
            
            MailboxProcessor.Start(fun inbox-> 
                
                let resetTimeSpan = TimeSpan.FromMinutes(1.0)
                let maxFailureCount = 3

                let rec messageLoop = async { // (cirtuitBreaker : CircuitBreakerState) = async {
        
                    let! msg = inbox.Receive()
                    let! res = sendFunction msg |> Async.Catch
                
                    match res with 
                        | Choice1Of2 _ -> return! messageLoop
                        | Choice2Of2(ex) -> 
                            internalErrorHandling(sprintf "Elasticsearch Appender fail: %O" ex)
                            return! messageLoop                    
                }

                messageLoop
            )
        
        

    type Appender(sendFunction : PostFunction<'a>) = 
        inherit BufferingAppenderSkeleton()
        
        let internalLogging = base.ErrorHandler.Error
        let mutable connectionInfo = {Server="";Index="";Port=0}

        let agent = 
            let sendWithBreaker = Helpers.sendFuncWithCircuitBreaker sendFunction (fun () -> connectionInfo) (fun msg -> internalLogging(msg))
            ProcessingAgents.mailboxProcessor sendWithBreaker (fun msg -> internalLogging(msg))

        new() = Appender(Communication.sendFunction())

        member val ConnectionString = "" with get, set
        
        override this.ActivateOptions() = 
            
            base.ActivateOptions()
            ServicePointManager.Expect100Continue <- false
            if String.IsNullOrEmpty(this.ConnectionString) then
                base.ErrorHandler.Error("ConnectionString is null or empty");
            else
                connectionInfo <- Helpers.parseConnectionString this.ConnectionString this.BufferSize

        override this.SendBuffer (events : LoggingEvent[]) = agent.Post events
            