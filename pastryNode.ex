defmodule PastryNode do
    use GenServer
         #start Node process
        def start(random_nodeId,b) do
            hash=:crypto.hash(:sha, to_string(random_nodeId)) |> Base.encode16 |> Convertat.from_base(16) |> Convertat.to_base(b+1)
           
            {:ok,pid} = GenServer.start_link(__MODULE__,hash)
            {pid,hash,random_nodeId}
        end
    
        
        def init(args) do  
            {:ok,%{:nodeId => args}}
        end
        def handle_cast({:updateLeafSet,larger_leaves,smaller_leaves,neighbors,routing_table,total_requests,total_rows,total_cols},state) do
            {_,state_larger_leaves}=Map.get_and_update(state,:larger_leaves, fn current_NodeId -> {current_NodeId,larger_leaves} end)
            {_,state_smaller_leaves}=Map.get_and_update(state,:smaller_leaves, fn current_NodeId -> {current_NodeId,smaller_leaves} end)
            {_,state_neighbors}=Map.get_and_update(state,:neighbors, fn current_NodeId -> {current_NodeId,neighbors} end)
            {_,state_routing_table}=Map.get_and_update(state,:routing_table, fn current_NodeId -> {current_NodeId,routing_table} end)
            {_,state_total_requests}=Map.get_and_update(state,:total_requests, fn current_NodeId -> {current_NodeId,total_requests} end)
            {_,state_hopcount}=Map.get_and_update(state,:hop_count, fn current_NodeId -> {current_NodeId,0} end)
            {_,state_number_rows}=Map.get_and_update(state,:number_rows, fn current_NodeId -> {current_NodeId,total_rows} end)
            {_,state_number_columns}=Map.get_and_update(state,:number_columns, fn current_NodeId -> {current_NodeId,total_cols} end)
            state=Map.merge(state,state_larger_leaves)
            state=Map.merge(state,state_smaller_leaves)
            state=Map.merge(state, state_neighbors)
            state=Map.merge(state, state_routing_table)
            state=Map.merge(state, state_total_requests)
            state=Map.merge(state, state_hopcount)
            state=Map.merge(state, state_number_rows)
            state=Map.merge(state, state_number_columns)
            {:noreply,state}
        end
    
        def handle_cast({:receive_request,nodeId,count,nodeListSize,processId,hash,b},state) do
           
            if(count<state[:total_requests]) do
            key=:crypto.hash(:sha,Project3.generateRandomNodeId(128,true))|> Base.encode16 |> Convertat.from_base(16) |> Convertat.to_base(b+1)
           
            GenServer.cast(self(),{:route,key,hash,b,0})
            
            GenServer.cast(self(),{:receive_request,nodeId,count,nodeListSize,processId,hash,b})
            
            end
            {:noreply,state}
    
        end
    
        def handle_cast({:route,key,hash_value_of_Node,b,hops},state) do
    
            #Combine the allvalues small and big and arrange in  increasing order
            allvalues=state[:larger_leaves]++state[:smaller_leaves]
            allvalues=Enum.sort(allvalues,fn(x,y) -> elem(x,1)<elem(y,1)  end)
    
            lowerVal=elem(Enum.at(allvalues,0),1)
            higherVal=elem(Enum.at(allvalues,length(allvalues)-1),1)
    
            if(key>=lowerVal and key<=higherVal) do
                
                GenServer.cast(Master_Server,{:msg_delivered,hops+1,self(),key})
            else
              
                row=longest_common_prefix(key,hash_value_of_Node,0,0)
                if(state[:number_rows]<= row) do
                   
                    nearest_neighbour=nearest_neighbour(key,state[:routing_table][state[:number_rows]-1],0,row,state[:number_columns])
                else
                    nearest_neighbour=nearest_neighbour(key,state[:routing_table][row],0,row,state[:number_columns])
                end
    
              
    
                if(elem(nearest_neighbour,2)==-1) do
                    combinedValues=allvalues++get_row(state[:routing_table],state[:number_rows],state[:number_columns],[],0,0)++state[:neighbors]
                    #IO.inspect combinedValues
                    nearest_node=no_nearest_matching_prefix_node(combinedValues,key,hash_value_of_Node,row)
                    #IO.puts "Nearest node for the given key #{inspect key} is #{inspect nearest_node} "
                    if(elem(nearest_node,2)==-1) do
                        
                        GenServer.cast(Master_Server,{:msg_delivered,hops,self(),key})
                    else
                        
                        GenServer.cast(elem(nearest_node,0),{:route,key,elem(nearest_node,1),b,hops+1})
                    end
                else
                   
                   
                    GenServer.cast(elem(nearest_neighbour,0),{:route,key,elem(nearest_neighbour,1),b,hops+1})
                    #IO.puts "Message"
    
                end
               
            end
    
           
            {:noreply,state}
    
        end
    
        def no_nearest_matching_prefix_node(combinedValues,kval,hashval,lower) do
         
            val=find_matching_prefix_node(combinedValues,kval,hashval,lower,0,length(combinedValues))
            val
        end 
    
        def find_matching_prefix_node(combinedValues,kval,hashval,lower,start_index,length) do
    
            val = {-1,-1,-1}
    
            if(start_index<length) do
                lcp=longest_common_prefix(elem(Enum.at(combinedValues,start_index),1),kval,0,0)
                {changed,_}=Float.parse(elem(Enum.at(combinedValues,start_index),1))
                {changed_kval,_}=Float.parse(kval)
                {changed_hashval,_}=Float.parse(hashval)
    
                if( lcp>=lower and :erlang.abs(changed-changed_kval)>:erlang.abs(changed_hashval-changed_kval) and start_index<length) do
                      
                        val=Enum.at(combinedValues,start_index)
                      
                else if(start_index<length) do
                   
                   val=find_matching_prefix_node(combinedValues,kval,hashval,lower,start_index+1,length)   
                else  
                
                     end
                end
    
            end
    
            val
        end
    
        def get_row(routing_table,number_rows,number_columns,list_of_routingTable,rowNumber,columnNumber) do
            if(rowNumber<number_rows) do
                table1=get_column(routing_table,number_rows,number_columns,list_of_routingTable,rowNumber,columnNumber)
                list_of_routingTable=list_of_routingTable++table1
                table2=get_row(routing_table,number_rows,number_columns,list_of_routingTable,rowNumber+1,columnNumber)
                list_of_routingTable=list_of_routingTable++table2
            end
    
            #IO.inspect list_of_routingTable
            list_of_routingTable
    
        end
    
    
        def get_column(routing_table,number_rows,number_columns,list_of_routingTable,rowNumber,columnNumber) do
             previousList=list_of_routingTable
             if(columnNumber<number_columns) do
                  if(elem(routing_table[rowNumber][columnNumber],2)!=-1) do
                     previousList=previousList++[routing_table[rowNumber][columnNumber]]
                     list_of_routingTable=previousList
                     list_of_routingTable=get_column(routing_table,number_rows,number_columns,list_of_routingTable,rowNumber,columnNumber+1)
                  end
            end
            list_of_routingTable
        end
    
        def longest_common_prefix(key,hash,start_index,longestCommonLength) do
            
            {hash,_}=Float.parse(hash)
            hash=hash|>trunc
            hash=to_string(hash)
            if(String.at(key,start_index) == String.at(hash,start_index)) do
             longestCommonLength=longest_common_prefix(key,hash,start_index+1,longestCommonLength+1)
            end
            longestCommonLength
        end
        
    
        def nearest_neighbour(key,row,columnIndex,longestCommonLength,num_columns) do
            valueOfNearestNeighbor={-1,-1,-1}
           if(columnIndex < num_columns) do
                if( elem(row[columnIndex],2) == -1 ) do
                     valueOfNearestNeighbor=nearest_neighbour(key,row,columnIndex+1,longestCommonLength,num_columns)
                else
                     if(String.at(key,longestCommonLength+1) != String.at(to_string(elem(row[columnIndex],1)),longestCommonLength+1)) do
                             valueOfNearestNeighbor=nearest_neighbour(key,row,columnIndex+1,longestCommonLength,num_columns)
                     else
                             valueOfNearestNeighbor=row[columnIndex]
                     end
                end
            end
            valueOfNearestNeighbor
           
        end
    
        def neighbors(node_list, index, start_index, neighbor_list) do
            if(start_index < 9 and length(node_list)!=0) do
                node_random = Enum.random(node_list)
                List.delete(node_list,node_random)
                if ( :erlang.abs(elem(node_random,2)-index)> 2 or :erlang.abs(elem(node_random,2)-index) < 2) do
                    neighbor_list = neighbor_list ++ [node_random]
                    
                end
                start_index=start_index+1
                neighbor_list=neighbors(node_list, index, start_index, neighbor_list)
            end
            neighbor_list
        end
        # Small Leaf Set of given node
        def smaller_leaves(node_list,index,b,setofsmaller_leaves) do
            if((index==1 or index-1<0 or index-1==0) and b>0)  do
                      if(index-1==0 and b>0) do
                            smaller_leaves=smaller_leaves(node_list,length(node_list),b,setofsmaller_leaves)
                      else if(index-1<0 and b>0) do
                         setofsmaller_leaves=setofsmaller_leaves++[Enum.at(node_list,length(node_list)-1)]
                         setofsmaller_leaves=smaller_leaves(node_list,length(node_list)-1,b-1,setofsmaller_leaves)
                      else if(index==1 and b>0) do
                              setofsmaller_leaves=setofsmaller_leaves++[Enum.at(node_list,length(node_list)-1)]
                              setofsmaller_leaves=smaller_leaves(node_list,length(node_list)-1,b-1,setofsmaller_leaves)
                           end
                        end
                      end
              else if(b>0) do
                      setofsmaller_leaves=setofsmaller_leaves++[Enum.at(node_list,index-1)]
                      setofsmaller_leaves=smaller_leaves(node_list,index-1,b-1,setofsmaller_leaves)
                   end
              end
              setofsmaller_leaves
          end
    
        # Compute the larger Leaf Set for given node
        def larger_leaves(node_list,index,b,setofLarger_leaves) do
            if((index==length(node_list) or index+1>length(node_list) or index+1==length(node_list)) and b>0)  do
                    if(index+1==length(node_list) and b>0) do
                          setofLarger_leaves=larger_leaves(node_list,-1,b,setofLarger_leaves)
                    else if(index+1>length(node_list) and b>0) do
                            setofLarger_leaves=setofLarger_leaves++[Enum.at(node_list,index)]
                            setofLarger_leaves=larger_leaves(node_list,-1,b-1,setofLarger_leaves)
                    else if(index==length(node_list) and b>0) do
                            setofLarger_leaves=setofLarger_leaves++[Enum.at(node_list,index)]
                            setofLarger_leaves=larger_leaves(node_list,-1,b-1,setofLarger_leaves)
                         end
                      end
                    end
            else if(b>0) do
                    setofLarger_leaves=setofLarger_leaves++[Enum.at(node_list,index+1)]
                    setofLarger_leaves=larger_leaves(node_list,index+1,b-1,setofLarger_leaves)
                 end
            end
            setofLarger_leaves
        end
    
        
    
        #Generate Routing Table for a given node
        def generateRoutingTable(b,hash_value_of_Node,nodelist) do
            numRows=round(:math.log(length(nodelist))/:math.log(:math.pow(2,b))) 
            numColumns=trunc(:math.pow(2,b)-1) 
            routing_table=%{}
            #IO.puts "number of Columns = #{inspect numColumns}" 
            #IO.puts "number of Rows = #{inspect numRows}" 
            routing_table=tableRows(nodelist,numColumns,numRows,0,hash_value_of_Node,routing_table)
            #IO.puts "routing_table=#{inspect routing_table}"
            routing_table
        end
    
    
        def tableRows(node_list,numColumns,numRows,rowNumber,hash_value_of_Node,routing_table) do
            if(rowNumber<numRows) do
                if(rowNumber==0) do
                    substr="";
                    table1=tableColumns(node_list,numColumns,numRows,rowNumber,0,hash_value_of_Node,substr,routing_table)
                    routing_table=Map.merge(routing_table,table1)
                    table2=tableRows(node_list,numColumns,numRows,rowNumber+1,hash_value_of_Node,routing_table)
                    routing_table=Map.merge(routing_table,table2)
    
                else
                    # IO.puts "#{rowNumber}"
                    substr=String.slice(hash_value_of_Node,0..rowNumber-1)
                    table1=tableColumns(node_list,numColumns,numRows,rowNumber,0,hash_value_of_Node,substr,routing_table)
                    routing_table=Map.merge(routing_table,table1)
                    table2=tableRows(node_list,numColumns,numRows,rowNumber+1,hash_value_of_Node,routing_table)
                    routing_table=Map.merge(routing_table,table2)
    
                end          
            end 
            routing_table
    
        end
    
        def tableColumns(node_list,numColumns,numRows,rowNumber,columnNumber,hash_value_of_Node,substr,routing_table) do
            previous =substr
            if(columnNumber<numColumns) do
                substr=substr<>to_string(columnNumber)
                result=find_matching_element(node_list,substr,hash_value_of_Node)
                   if(routing_table[rowNumber]!=nil) do
                        {_,routingtableUpdated}=Map.get_and_update(routing_table,rowNumber,fn current_NodeId -> {current_NodeId,Map.merge(current_NodeId,%{columnNumber => result})} end)
                        routing_table=Map.merge(routing_table,routingtableUpdated)             
                   else
                       {_,routingtableUpdated}=Map.get_and_update(routing_table,rowNumber,fn current_NodeId -> {current_NodeId, %{columnNumber => result}} end)
                       routing_table=Map.merge(routing_table,routingtableUpdated)
                   end
                routing_table=tableColumns(node_list,numColumns,numRows,rowNumber,columnNumber+1,hash_value_of_Node,previous,routing_table)
            end
            routing_table
    
        end
    
        def find_matching_element(node_list,substr,hash_value_of_Node) do
            match_list={}
            match_list=recursive(node_list,substr,0,match_list,hash_value_of_Node)
            if tuple_size(match_list) <=0 do
                match_list={-1,-1,-1}
            end
            match_list
        end
    
        def recursive(node_list,substr,index,val,hash_value_of_Node) do
            if(index<length(node_list)) do
                 if(String.starts_with?(elem(Enum.at(node_list,index),1),substr) and 
                    elem(Enum.at(node_list,index),1)!=hash_value_of_Node) do
                    val=Enum.at(node_list,index)
                 else
                    val = recursive(node_list,substr,index+1,val,hash_value_of_Node)
                 end
            end
            val
        end
    
    end
    