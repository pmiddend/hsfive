module HsFive.Util where

dropEverySecond :: [a] -> [a]
dropEverySecond [] = []
dropEverySecond [x] = [x]
dropEverySecond (x : _ : xs) = x : dropEverySecond xs
