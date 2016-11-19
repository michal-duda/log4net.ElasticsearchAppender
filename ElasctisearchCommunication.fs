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

    let private appendLine (event : LoggingEvent) (sb : StringBuilder) =
        sb.AppendLine("{\"index\" : {} }").AppendLine(JsonConvert.SerializeObject(event))

    let private buildContent (events : LoggingEvent[]) : String = 
        let serialized = events |> Array.fold (fun acc event -> appendLine event acc) (StringBuilder())
        serialized.ToString()

    let sendFunction() : PostFunction<HttpStatusCode> = 
        fun (connectionInfo : ConnectionInfo) (events : LoggingEvent[]) ->
            let req = WebRequest.CreateHttp (connectionInfo.GetUrlString())
            req.Method <- "POST"
            req.ProtocolVersion <- HttpVersion.Version10
            let postBytes = buildContent events |> System.Text.Encoding.ASCII.GetBytes
            req.ContentLength <- postBytes.LongLength
            req.ContentType <- "application/xml; charset=utf-8"
            req.Timeout <- 1500
            async{
                Console.WriteLine("About to send events")
                use reqStream = req.GetRequestStream()
                do! reqStream.WriteAsync(postBytes, 0, postBytes.Length) |> Async.AwaitIAsyncResult |> Async.Ignore
                reqStream.Close()
                use! res = req.AsyncGetResponse()
                let httpRes = res :?> HttpWebResponse
                return httpRes.StatusCode
            }

//         async {
//            let request = new HttpRequestMessage(HttpMethod.Post, connectionInfo.GetUrlString())
//            request.Content <- buildContent events
//            Console.WriteLine("About to send events")
//            return! httpClient.SendAsync(request) |> Async.AwaitTask
//        }