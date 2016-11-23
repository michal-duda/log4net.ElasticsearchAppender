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
        let serialized = events |> Array.fold (fun acc event -> appendLine event acc) (StringBuilder())
        serialized.ToString()

    let sendFunction() : PostFunction<HttpStatusCode> = 
//        fun (connectionInfo : ConnectionInfo) (events : LoggingEvent[]) ->
//            let req = WebRequest.CreateHttp (connectionInfo.GetUrlString())
//            req.Method <- "POST"
//            req.ProtocolVersion <- HttpVersion.Version10
//            let postBytes = buildContent events |> System.Text.Encoding.ASCII.GetBytes
//            req.ContentLength <- postBytes.LongLength
//            req.ContentType <- "application/xml; charset=utf-8"
//            req.Timeout <- 1500
//            async{
//                Console.WriteLine("About to send events")
//                use reqStream = req.GetRequestStream()
//                do! reqStream.WriteAsync(postBytes, 0, postBytes.Length) |> Async.AwaitIAsyncResult |> Async.Ignore
//                reqStream.Close()
//                use! res = req.AsyncGetResponse()
//                let httpRes = res :?> HttpWebResponse
//                return httpRes.StatusCode
//            }
        let httpClient = new HttpClient()
        fun (connectionInfo : ConnectionInfo) (events : LoggingEvent[]) ->
         
//            let request = new HttpRequestMessage(HttpMethod.Post, connectionInfo.GetUrlString())
//            request.Content <- new StringContent(buildContent events)
//            request.Method <- HttpMethod.Post
            Console.WriteLine("About to send events")
            let task = httpClient.PostAsync(connectionInfo.GetUrlString(),(new StringContent(buildContent events)), (new CancellationTokenSource(2000)).Token)
            Console.WriteLine("After send events")
            Async.FromContinuations(fun (cont, econt, ccont) -> 
                task.ContinueWith(fun (t:Task<HttpResponseMessage>) -> 
                    match t with
                    | _ when t.IsFaulted  -> econt t.Exception
                    | _ when t.IsCanceled -> 
                        // note how this uses error continuation 
                        // instead of cancellation continuation
                        econt (new Exception())
                    | _ -> cont t.Result.StatusCode) |> ignore)

        