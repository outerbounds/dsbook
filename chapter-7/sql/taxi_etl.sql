SELECT * FROM nyc_taxi
	WHERE hour(from_unixtime(pickup_at / 1000)) BETWEEN 9 AND 17
