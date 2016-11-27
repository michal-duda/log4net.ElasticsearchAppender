namespace log4net.ElasticsearchAppender

module Communication = 

    open log4net.Core
    open System.Threading.Tasks
    open System.Net.Http
    open Types
    open System
    open System.Text
    open Newtonsoft.Json
    open System.Runtime.CompilerServices
    open System.Net
    open System.IO
    open System.Threading

    let private appendLine (event : LoggingEvent) (sb : StringBuilder) =
        sb.AppendLine("{\"index\" : {} }").AppendLine(JsonConvert.SerializeObject(event))

    let private buildContent (events : LoggingEvent[]) : String = 
        if events.Length > 1 then
            let serialized = events |> Array.fold (fun acc event -> appendLine event acc) (StringBuilder())
            serialized.ToString()
        else
            JsonConvert.SerializeObject(events.[0])

    let sendFunction() : PostFunction<HttpStatusCode> = 

        let httpClient = new HttpClient()
        fun (connectionInfo : ConnectionInfo) (events : LoggingEvent[]) ->
            let request = new HttpRequestMessage(HttpMethod.Post, connectionInfo.GetPostUrlString(events.Length))
            request.Content <- (new StringContent(buildContent events, Encoding.UTF8, "application/json"))
            let task = httpClient.SendAsync(request, (new CancellationTokenSource(2000)).Token)
            Async.FromContinuations(fun (cont, econt, ccont) -> 
                task.ContinueWith(fun (t:Task<HttpResponseMessage>) -> 
                    match t with
                    | _ when t.IsFaulted  -> econt t.Exception
                    | _ when t.IsCanceled -> 
                        // note how this uses error continuation 
                        // instead of cancellation continuation
                        econt (new Exception())
                    | _ -> cont t.Result.StatusCode) |> ignore)

        