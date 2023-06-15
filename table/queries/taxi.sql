SELECT
  ride_status,
  COUNT(*) AS num_rides,SUM(passenger_count) AS total_passengers
FROM PCOLLECTION
  WHERE NOT ride_status = 'enroute'
  GROUP BY ride_status