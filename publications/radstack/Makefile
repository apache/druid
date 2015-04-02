all : radstack.pdf

clean :
	@rm -f *.aux *.bbl *.blg *.log

%.tex : %.bib

%.pdf : %.tex %.bib
	lualatex $(*F)
	bibtex $(*F)
	lualatex $(*F)
	lualatex $(*F)
