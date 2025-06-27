module HsFive.UtilityFunctions where

-- Shamelessly copied from
-- https://hackage.haskell.org/package/monad-loops-0.4.3/docs/src/Control-Monad-Loops.html#unfoldWhileM
unfoldWhileM :: (Monad m) => (a -> Bool) -> m a -> m [a]
unfoldWhileM p m = loop id
  where
    loop f = do
      x <- m
      if p x
        then loop (f . (x :))
        else return (f [])
