## Lab2A Leader Election
實作 Leader 選舉的機制，結構體參數的部分我直接參考論文設定，實作上有幾點比較重要的是:
1. 要在正確的時刻重設 election timer
    - 轉變為 Follower 跟 Candidate 兩個狀態時，這點比較直觀
    - Follower 收到 "合法" AppendEntries RPC 時，合法指得是請求的 Term >= CurrentTerm，否則代表該請求是由一個過期的 Leader 發送，那重設 timer 反而會觸發不必要的重新選舉
    - Follower 收到 "合法" RequestVote RPC 時，合法指得是
        - 請求的 Term >= CurrentTerm
        - Follower 在該 Term 還沒投過票 (rf.votedFor == -1)
        - 請求的 log 是 up-to-date 的，代表 Candidate 的 log 至少不落於人後，up-to-date 的定義請參閱論文 5-4 Safety 章節中有關 Election Restriction 的部分
    - 成為 Leader 之後應該要將 election timer 停止
2. 無論是任何 RPC 的 request or response，只要參數中 Term > CurrentTerm，就轉變身分為 Follower，因為節點已經過期了，而且這樣才能讓各個節點盡快收斂到同一個 Term
    > If RPC request or response contains term T > CurrentTerm:
set CurrentTerm = T, convert to follower
3. 任何過期的請求都直接忽略即可；而 Candidate 在收選票時，要確保 peer 的 Term == CurrentTerm 才能接收選票
4. 變成 Leader 時記得要定期發送心跳包並重設 heartbeat timer
5. RPC 記得要用 goroutine 異步地發送，避免卡住節點的其他操作


另外在 debug 的過程中深刻體會到 [Raft Locking Advice](https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt) 提到的一點，**不要用 Lock 包住任何關於 Channel or RPC 的操作**，我一開始在實作發送心跳包的時候沒注意到，測試了半天每次都是第 3 個測資沒法過，而且都只能選出 2 次 Leader，後來仔細觀察 log 內容才發現 Leader 節點幾乎在開始發送第二次心跳包之後就沒有 log 了，這才恍然大悟難怪到後面沒法選出新 Leader，因為 Leader 可能發送 RPC 給斷線的節點，等不到回應就發生 deadlock 了 XD (or 持有鎖很長一段時間沒法釋放)

### Test Result
經過 1000 測試都 Pass    
![image](https://hackmd.io/_uploads/rJKOOaR0A.png)
