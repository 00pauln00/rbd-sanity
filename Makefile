rbd-sanity : rbd-sanity.c
	gcc -g -o rbd-sanity rbd-sanity.c -Wall -Wextra \
	-lrbd -lrados -lpthread -laio

asan :
	gcc -g -o rbd-sanity rbd-sanity.c -Wall -Wextra -fsanitize=address \
	-lrbd -lrados -lpthread

clean :
	rm -f rbd-sanity
