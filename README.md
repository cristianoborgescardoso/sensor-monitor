# Sensor Monitor

```powershell
 $endpoint = New-Object System.Net.IPEndPoint ([System.Net.IPAddress]::Loopback, 3344); $client = New-Object System.Net.Sockets.UdpClient; $bytes = [System.Text.Encoding]::UTF8.GetBytes("sensor_id=t5; value=35.5"); $client.Send($bytes, $bytes.Length, $endpoint); $client.Close()
```
## Benchmark
```bash
nping --udp -p 3344 --data-string "sensor_id=t29; value=35.5" -c 50000 --rate 2000 127.0.0.1
```
## Build and Run
```bash
docker-compose up -d --build
```
