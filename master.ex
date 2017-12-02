defmodule Master do
    use GenServer
    @b 2
    
    def init(:ok) do
            {:ok,%{}}
    end
    def start_master(input) do
        serverConfig=String.to_atom(to_string("server@")<>elem(input,0))
        {:ok,_}=Node.start(serverConfig)
        cookie=Application.get_env(:project3, :cookie)
        #start master server
        {:ok,_} = GenServer.start_link(__MODULE__, :ok, name: Master_Server)  
        Node.set_cookie(cookie)
        numNodes=String.to_integer(to_string(Enum.at(elem(input,1),0)))
        numRequest=String.to_integer(to_string(Enum.at(elem(input,1),1)))
        iterate=1..numNodes
        nodes_with_pids=spawn_nodes(numNodes,1,[],[],Enum.to_list(iterate))
        nodes_with_pids=Enum.sort(nodes_with_pids,fn(x,y) -> elem(x,2)<elem(y,2)  end)
        #IO.inspect nodes_with_pids
        GenServer.cast(Master_Server,{:update_master_state,numRequest,numNodes,nodes_with_pids})
        generateTopology(nodes_with_pids,numRequest)
        peerToPeer(nodes_with_pids, numNodes, 0)
        end

    def handle_cast({:update_master_state,numRequest,numNodes,nodes_with_pids},state) do
            {_,state_numRequest}=Map.get_and_update(state,:numRequest, fn current_NodeId -> {current_NodeId,numRequest} end)
            {_,state_numNodes}=Map.get_and_update(state,:numNodes, fn current_NodeId -> {current_NodeId,numNodes} end)
            {_,state_nodes_with_pids}=Map.get_and_update(state,:nodes_with_pids, fn current_NodeId -> {current_NodeId,nodes_with_pids} end)
            {_,state_hops_count}=Map.get_and_update(state,:hop_count, fn current_NodeId -> {current_NodeId,0} end)
            {_,state_requestsReceived}=Map.get_and_update(state,:requestsReceived, fn current_NodeId -> {current_NodeId,0} end)
            state=Map.merge(state,state_numRequest)
            state=Map.merge(state,state_numNodes)
            state=Map.merge(state,state_nodes_with_pids)
            state=Map.merge(state,state_hops_count)
            state=Map.merge(state,state_requestsReceived)
            {:noreply,state}
    
    end
    
    def handle_cast({:msg_delivered,hopcount,process,hash},state) do
         {_,state_hopcount}=Map.get_and_update(state,:hop_count, fn current_NodeId -> {current_NodeId,current_NodeId+hopcount} end)
         state=Map.merge(state,state_hopcount)
         {_,state_requestsReceived}=Map.get_and_update(state,:requestsReceived, fn current_NodeId -> {current_NodeId,current_NodeId+1} end)
         state=Map.merge(state,state_requestsReceived)
    
         if(state[:requestsReceived]>=(state[:numRequest] * state[:numNodes])) do
            average_hop_count=state[:hop_count]/(state[:numRequest] * state[:numNodes])
            IO.puts "average hop count = #{inspect average_hop_count}"
            Process.exit(self(),:normal)
         end
    
        {:noreply,state}
    
    end
    def generateTopology(nodes_with_pids,numRequest) do
        
                Enum.each(Enum.with_index(nodes_with_pids),fn(x)->
                        neighbors = PastryNode.neighbors(nodes_with_pids, elem(x,1), 0, [])    
                        larger_leaves=PastryNode.larger_leaves(nodes_with_pids,elem(x,1),@b,[])
                        smaller_leaves=PastryNode.smaller_leaves(nodes_with_pids,elem(x,1),@b,[])
                        routing_table=PastryNode.generateRoutingTable(@b,elem(elem(x,0),1),nodes_with_pids)
                        GenServer.cast(elem(elem(x,0),0),{:updateLeafSet,larger_leaves,smaller_leaves,neighbors,routing_table,numRequest,round(:math.log(length(nodes_with_pids))/:math.log(:math.pow(2,@b))),trunc(:math.pow(2,@b)-1)})
                 end)
        
        end
     
     def peerToPeer(nodeList, numNodes, c) do
            if(c < numNodes) do
            count = 0
            c = c+1 
            random_node = Enum.random(nodeList) 
            List.delete(nodeList,random_node)
            nodeListSize = numNodes
            processId = elem(random_node,0)
            hash = elem(random_node,1)
            nodeId = elem(random_node,2)
            GenServer.cast(processId,{:receive_request,nodeId,count,nodeListSize,processId,hash,@b})
            peerToPeer(nodeList, numNodes, c)
            end
     end
    
     def spawn_nodes(numNodes,start_index, list1 ,nodeIdSpaceUsed,nodeIdSpace) do
                 if(start_index<=numNodes) do
                    random_nodeId=Enum.random(nodeIdSpace)
                    list1=list1++[PastryNode.start(random_nodeId,@b)]
                    start_index=start_index+1
                    List.delete(nodeIdSpace,random_nodeId)
                    list1=spawn_nodes(numNodes,start_index,list1,nodeIdSpaceUsed,nodeIdSpace)
                 end
                 list1
    end
    
    
    
    end