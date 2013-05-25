reset
set terminal postscript eps enhanced monochrome 26
set output "kmeans.eps"
#set term x11 0
set boxwidth 0.9 absolute
set key inside right top vertical noreverse noenhanced autotitles nobox
set style histogram clustered gap 1 title offset character 0, 0, 0
set datafile missing '-'
set style data histograms
set xlabel offset 0,0.5 "Data points (10^5)"
set ylabel offset 2,0 "Time (seconds)"
set nomxtics
set mytics -1
set grid noxtics noytics
set autoscale y
set autoscale y2
set grid xtics ytics
set title "K=3,Iteration=5"
set key autotitle columnhead

plot "kmeans.data" using 1:2 with linespoints lw 4 pt 1 ps 1, \
	"kmeans.data" using 1:3 with linespoints lw 4 pt 2 ps 1, \
	"kmeans.data" using 1:4 with linespoints lw 4 pt 3 ps 1, \
	"kmeans.data" using 1:5 with linespoints lw 4 pt 4 ps 1, \
	"kmeans.data" using 1:6 with linespoints lw 4 pt 5 ps 1, \
	"kmeans.data" using 1:7 with linespoints lw 4 pt 6 ps 1