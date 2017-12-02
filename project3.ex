defmodule Project3 do

 def main(args \\ []) do

        if(length(args)==2) do
            #Create the server Processs
            get_ip(args)|>Master.start_master
            loop()
        else
            IO.puts "Enter the arguments"
        end 
end
def get_ip(args) do
   {:ok,[{ipadd1,_,_},{_,_,_}]}=:inet.getif()
   ip_address= ipadd1|> Tuple.to_list |> Enum.join(".") 
   {ip_address,args}
end
def generateRandomNodeId(length, type \\ :all) do
    numbers = "0123456789"
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    

    generated_list =
      cond do
        true -> letters <> String.downcase(letters) <> numbers
      end
      |> String.split("", trim: true)

    recursivelyGenerate(length, generated_list)
  end

  defp get_range(length) when length > 1, do: (1..length)
  defp get_range(length), do: [1]

  defp recursivelyGenerate(length, generated_list) do
    get_range(length)
    |> Enum.reduce([], fn(_, acc) -> [Enum.random(generated_list) | acc] end)
    |> Enum.join("")
  end
def loop() do
   loop()
end

end
