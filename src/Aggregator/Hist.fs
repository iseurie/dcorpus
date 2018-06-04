namespace Aggregator
open Discord
open Discord.Rest
open Deedle
open System.Linq

module History =
    module Extractor = 
        type T<'r, 'c when 'c : equality and 'r : equality> =
            (IMessage -> Frame<'r, 'c>)
        
    module Spec =
        type Rel = First | Last | From of uint64

        type T =
            { direction  : Discord.Direction;
              limit      : uint64;
              relativeTo : Rel
            }
        let Default =
            { direction  = Direction.Before;
              limit      = 1024UL*256UL
              relativeTo = Rel.First
            }

    let Corpus<'r, 'c when 'r : equality and 'c : equality>
        (gid:uint64, http:DiscordRestClient,
         spec:Spec.T, opt:RequestOptions,
         extractor:Extractor.T<'r, 'c>) =
        async {
            let! guild = http.GetGuildAsync gid |> Async.AwaitTask
            let! chans =
                guild.GetTextChannelsAsync(RequestOptions.Default)
                |> Async.AwaitTask

            let messageStreams =
                let chf (ch:RestTextChannel) =
                    let n, dir = spec.limit, spec.direction
                    match spec.relativeTo with
                        | Spec.First -> ch.GetMessagesAsync(System.UInt64.MaxValue, dir, int n, opt)
                        | Spec.Last -> ch.GetMessagesAsync(0UL, dir, int n, opt)
                        | Spec.From id -> ch.GetMessagesAsync(id, dir, int n, opt)

                Seq.map chf <| chans

            let! messagePool =
                messageStreams
                |> AsyncEnumerable.Concat
                |> AsyncEnumerableExtensions.Flatten
                |> Async.AwaitTask

            return messagePool |> Seq.map extractor |> Frame.mergeAll
        }
