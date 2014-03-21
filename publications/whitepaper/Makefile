all : druid.pdf

sigmod : sgmd0658-yang.zip

%.zip : %.pdf
	rm -f dummy.ps
	touch dummy.ps
	zip $@ $*.pdf $*.tex dummy.ps
	rm -f dummy.ps

clean :
	@rm -f *.aux *.bbl *.blg *.log dummy.ps *.zip

%.tex : %.bib

%.pdf : %.tex %.bib
	lualatex $(*F)
	bibtex $(*F)
	lualatex $(*F)
	lualatex $(*F)
