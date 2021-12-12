# NJU-DisSys-2017
This is the resource repository for the course Distributed System, Fall 2017, CS@NJU.

In Assignment 2 and Assignment 3, you should primarily focus on /src/raft/...

# 环境配置
- 设置 Project GOPATH 为当前项目的根目录
# Assignment 1
- RequestVoteArgs and RequestVoteReply structs: 参考 Figure 2
- RequestVote RPC handler: RequestVote()
    - 向所有的 peers sendRequestVote (需要排除自己 me)
    - if ok, 那么读取 reply 里头的 Term 和 VoteGranted
    - 如果超过半数同意，则需要修改这个节点的 状态（Persister?) 和 currentTerm