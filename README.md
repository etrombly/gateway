This is a gateway script to communicate between RFM69 nodes and an MQTT broker. Ported from https://github.com/computourist/RFM69-MQTT-client to python.

Prereqs:
  you also need the python paho library for mqtt.

  You also need my RFM69 library to be in the gateway directory:
  
  cd gateway; git clone https://github.com/etrombly/RFM69
  
  also the prereqs mentioned on that page.
  


example demo.sitemap:

sitemap demo label="Main Menu"

```
{
        Frame label="Gateway"{
                Text item=item_uptime_mqtt
                Switch item=item_power_kitchen
        }
}
```

example demo.items:

```
Number item_uptime_mqtt "Gateway Uptime [%d minutes]" (ALL) {mqtt="<[mymosquitto:home/rfm_gw/nb/node01/dev00:state:default]"}
Switch item_power_kitchen "Kitchen Power" (ALL) {mqtt=">[mymosquitto:home/rfm_gw/sb/node02/dev17:command:*:default]"}
String item_uptime_update "Update uptime" (ALL) {mqtt=">[mymosquitto:home/rfm_gw/sb/node01/dev00:command:*:default]"}
String item_power_kitchen_status {mqtt="<[mymosquitto:home/rfm_gw/nb/node02/#:state:default]"}
```

example demo.rules:

```
rule "Update uptime"
        when
                Time cron "0 0/1 * * * ?"
        then
                sendCommand(item_uptime_update, "READ")
end

rule "Update Kitchen power"
        when
                Item item_power_kitchen_status received update
        then
                if (item_power_kitchen_status.state != "ON"){
                        postUpdate(item_power_kitchen, OFF)
                }
end
```

The power switch isn't currently connected, so when you try to turn it on you will get a link error message, which will automatically turn the switch back off.