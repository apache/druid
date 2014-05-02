all : druid sigmod zip

druid : druid.pdf

sigmod : modii658-yang.pdf

zip : modii658-yang.zip

%.zip : %.pdf
	@rm -f dummy.ps
	@echo 1234 > dummy.ps
	zip $@ $*.pdf $*.tex dummy.ps

clean :
	@rm -f *.aux *.bbl *.blg *.out *.log dummy.ps *.zip

%.tex : %.bib

%.pdf : %.tex %.bib
	lualatex $(*F)
	bibtex $(*F)
	lualatex $(*F)
	lualatex $(*F)
