'use client'

import { LS_PREFIX } from '$lib/types/localStorage'

import { useLocalStorage } from '@mantine/hooks'
import { Accordion, AccordionDetails, AccordionSummary, Box, Button, Link, Typography } from '@mui/material'

const WelcomeCard = () => {
  const [welcomed, setWelcomed] = useLocalStorage<boolean>({
    key: LS_PREFIX + 'home/welcomed'
  })
  return (
    <>
      <Accordion expanded={!(welcomed ?? true)} onChange={() => setWelcomed(!welcomed)} disableGutters sx={{}}>
        <AccordionSummary
          expandIcon={
            !welcomed ? (
              <i className={`bx bx-x`} style={{ fontSize: 24 }} />
            ) : (
              <i className={`bx bx-chevron-down`} style={{ fontSize: 24 }} />
            )
          }
          aria-controls='panel1-content'
          id='panel1-header'
          sx={{
            alignItems: 'start',
            p: 2,
            '.MuiAccordionSummary-content': { m: 0 },
            '.MuiAccordionSummary-expandIconWrapper': { p: 1 }
          }}
        >
          <Box sx={{ display: 'flex', flexWrap: 'wrap', width: '100%' }}>
            <Typography
              variant='h6'
              sx={{ px: 3, mr: 'auto', my: 4, display: 'flex', xs: { width: '100%' }, sm: { width: 'auto' } }}
            >
              Welcome to Feldera!
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'nowrap' }}>
              <Button
                href='https://www.feldera.com/docs/tour/'
                onClick={e => e.stopPropagation()}
                target='_blank'
                rel='noreferrer'
              >
                Take the tour
              </Button>
              <Button href='/demos/' onClick={e => e.stopPropagation()} LinkComponent={Link}>
                Try the demos
              </Button>
            </Box>
          </Box>
        </AccordionSummary>
        <AccordionDetails
          sx={{ aspectRatio: 1.77, p: 0, overflow: 'hidden', '.ytp-chrome-top ytp-show-cards-title': { width: 0 } }}
        >
          <iframe
            width='100%'
            height='100%'
            src='https://www.youtube.com/embed/tMLg9NyM3xk?showinfo=0&modestbranding=1'
            style={{ borderStyle: 'none' }}
            title='YouTube video player'
            allow='accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share'
            allowFullScreen
          ></iframe>
        </AccordionDetails>
      </Accordion>
    </>
  )
}

export const WelcomeTile = () => {
  return (
    <>
      <WelcomeCard></WelcomeCard>
    </>
  )
}
