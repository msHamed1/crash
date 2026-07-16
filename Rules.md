1- We have 5 resources . RabitMQ , Game Engine , Wss Gateway , Db worker . client . 

Rules : 
Player Join table . Recives Current State. 

Player Disconnect Removed from table. 

Player Place Bet -> Gateway -> RabbitMQ - > Game Engine -> ACK to RabbitMQ - > 
Game Engine process the Bet Command in Memory : 
A- Validation if OK . Create bet in memory as the first source of truth -> ASYNC send write it in the DB -> and send accpeted event to the player . 
B- Validation Not OK and cant create bet in memroy . send rejected event to player.

 IF A : and bet is created in memory and for some reason [ Operator cant confrim the bet or our db cant create the bet ]=> Update the memroy state and infrom the player that we cant place your bet internal error or external error . log that error and close the bet in memory or cancel it in our db . 
 

##Game event are queued in memory channel . one reader will read those events like [Round started , round crashed , player join, Player disconnect, round tick , Bet , Autocashout check on each tick ,Cashout Requests from player , Settle Crashed Round open bets ] 
Process is first in memory then send to db worker and operator . 
 once a bet is accpeted by the operator on Place phase and round has started. THe bet MUST be settled for win or lose or .. .. the bet CANT be Canceld on our end.
 DB wotker must ensure the update is written to our db .after in memory bet settles . player must be updated . db worker must write the reuslt ( and Keep trying to write to db ) if can't than means it needs a human action to see what happend !!.
 

If engine died . a new engien will pick up the open bets for the last round on that table from the DB and then canceld them and inform the operator -> Remeber we accpet the bet in memory and send it to db worker to write it and api worker to infrom the operator about it . 