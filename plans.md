Make a Object-Oriented method to get tweets per user
- each object will be attached to a specific user
- will have thousands of objects, will need some container object for subsets?
For tweet grabbing:
- need some way to make sure we dont miss any tweets without abusing API limits
- once we have proof of concept, we can request acadmeic access
- how expensive to run this on a server (probably not a lot, it needs very little computational power, just uptime and storage). Where can we secure funding? Will blockhain version need this as well?

Do we want a stream or our own stream? How good is the twitter stream?
Can we attribute tweets to an account from stream?

ADD Ability to add rules on the fly
FIX what we do to data from stream
TEST with data bunch of users

ADD TESTS

ADD MULTITHREADING

CONSIDER MAKING CLASS/OOP?
    would allow us to keep track of rules and such offline, and reduce API calls
    Would also allow us to keep track of our own things more easily