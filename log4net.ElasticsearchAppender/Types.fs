namespace log4net.ElasticsearchAppender

module Types = 
    open System.Threading.Tasks
    open log4net.Core
    open System
    open System.Runtime.CompilerServices
    
    type ConnectionInfo = 
        {Server:String; Index: String; Bulk: bool; Port:int}
        member this.GetUrlString() = String.Format("{0}:{1}/{2}/{3}", this.Server, this.Port, this.Index, (if this.Bulk then "_bulk" else ""))

    type PostFunction<'a> = ConnectionInfo -> LoggingEvent[] -> Async<'a>

