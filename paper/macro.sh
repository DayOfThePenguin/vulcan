#! /bin/bash
pdflatex final-paper.tex
biber final-paper.bcf
pdflatex final-paper.tex
open final-paper.pdf
shopt -s extglob
rm final-paper.!(pdf|tex)
