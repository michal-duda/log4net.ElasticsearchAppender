namespace log4net.ElasticsearchAppender

module Types = 
    open System.Threading.Tasks
    open log4net.Core
    open System
    open System.Runtime.CompilerServices
    
    type ConnectionInfo = 
        {Server:String; Index: String; Port:int}
        member this.GetPostUrlString(eventsCount : int) = String.Format("{0}:{1}/{2}/event/{3}", this.Server, this.Port, this.Index, (if eventsCount > 1 then "_bulk" else ""))

    type PostFunction<'a> = ConnectionInfo -> LoggingEvent[] -> Async<'a>

