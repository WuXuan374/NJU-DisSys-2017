# NJU-DisSys-2017
This is the resource repository for the course Distributed System, Fall 2017, CS@NJU.

In Assignment 2 and Assignment 3, you should primarily focus on /src/raft/...

# IDE 使用
- Shift + Command + F: Find in files
# 环境配置
- 设置 Project GOPATH 为当前项目的根目录
# Assignment 1
## 先梳理一下包括选举阶段整体的流程
- GoRoutine: 所有的节点都是 follower, 不断的接受 leader 发来的 heartbeat
  - heartbeat 的回复
    - false if term < currentTerm
- heartbeat 超时的话
  - follower 节点变成 candidate, 参加选举
    - currentTerm += 1
    - 给自己投票， votedFor = me, voteCount += 1
    - reset election timer
    - 发送 RequestVote RPC
  - 需要有一个数据结构 role: "leader" | "candidate" | "follower"
  - follower: 
    - false if term < currentTerm
    - true if votedFor null (当前 term 还没有投过票)
  - candidate:
    - 如果收到的 response 中 term > currentTerm
      - currentTerm = term; votedFor = null; 变回 follower --> role = "follower", 不管选举的结果了
    - 收到了多数票: 成为 Leader
    - election timeouts: 再次 election
    - 收到 Leader 的 appendEntries: 成为 follower
- Leader:
  - heartbeat timeout: 每次到时间就发送空的 AppnedEntries (暂时先不考虑日志), 这个 timeout 应该小于 election timeout
  - 无论什么 RPC, 如果收到的 term > currentTerm， 就变成 follower

## RPC
- RequestVoteArgs and RequestVoteReply structs: 参考 Figure 2
- RequestVote RPC handler: RequestVote()
    - 向所有的 peers sendRequestVote (需要排除自己 me)
    - if ok, 那么读取 reply 里头的 Term 和 VoteGranted
    - 如果超过半数同意，则需要修改这个节点的 状态（Persister?) 和 currentTerm
## heartbeat 机制的相关实现
- 首先在 Raft 的结构体里头，应该有计时器
  - time.Ticker?
  - candidate 的计时器，时间范围是 150-300 (取一个这个范围内的随机数？而且是每次要取不同的). 超出这个时间范围，没收到 heartbeat, 就变成 candidate
  - leader 的计时器，时间范围应该少于 150？(统一 100ms 干脆)， 这样 follower 能够及时收到 heartbeat
- https://zhuanlan.zhihu.com/p/128622498: 这篇文章推荐通过 Time.sleep() 完成 timeout
- 在 Raft struct 里头维护上一次收到 heartbeat 的时间/上一次发送 heartbeat 的时间
- 利用 Time.sleep() 周期性的去检查是否超时
- background goroutine?
## 一些编程方面的注意点
- 所有状态的检查: GetState(); 需要加锁
- 读取 RPC 的返回值: 这显然是一个异步操作，不能够直接读取结果
  - 读取 RequestVote 的结果
  - 读取 AppendEntries 的结果 (包括 heartbeat)
- 把 Log 全部加上！
## 面向测试编程
- 目前的问题出现在节点死亡的时候
  - 这个 RPC 会阻塞，导致是否收到多数票的判断也失败；那么就无法决出 Leader
  - "读取 RPC 的返回值: 这显然是一个异步操作，不能够直接读取结果"， 大抵确实是这个问题罢